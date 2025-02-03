package com.minsait.indation.silver

import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.{DateUtils, FileNameUtils, SilverConstants}
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.datalake.exceptions.NewAndModifyException
import com.minsait.indation.metadata.models.enums.{PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.metalog.models.{DatasetLog, Ingestion, SourceLog}
import com.minsait.indation.metalog.{MetaInfo, MetaLogManager}
import com.minsait.indation.silver.helper.{PartitionHelper, SchemaHelper, ValidationHelper}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._

import java.io.{PrintWriter, StringWriter}

class SilverEngine(val datalakeManager: DatalakeManager, val metaLogManager: MetaLogManager)
      extends Logging with SparkSessionWrapper{

  /* metalogManager Spark session*/
  private var source: Source = _
  private var ingestionLog: Ingestion = _
  private var sourceLog: SourceLog = _
  private var datasetLog: DatasetLog = _

  def process(source: Source): Ingestion = {

    logger.info("Init processing SilverEngine for source " + source.name)

    this.source = source

    if (metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, source.sourceId.get)) {
      logger.warn("Source " + source.name + " processed with result: Finish execution cause exists running ingestion")
      return Ingestion(layer = Some(MetaInfo.Layers.SilverLayerName))
    }

    logger.info("Initializing ingestionLog")
    this.ingestionLog = metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)
    logger.info("IngestionLog initialized with id " + ingestionLog.ingestionId)

    logger.info("Initializing SourceLog")
    this.sourceLog = metaLogManager.initSourceLog(ingestionLog.ingestionId, source.sourceId.get)
    logger.info("SourceLog initialized with id " + sourceLog.sourceLogId)

    logger.info("Getting Pending Silver data")
    val pendingSilver = metaLogManager.getPendingSilver(source.sourceId.get)
    logger.info("" + pendingSilver.length + " pending ingestion founds")

    try {
      initProcessIngestionLoop(pendingSilver)
    } catch {
      case ex: Exception =>
        logger.error(s"Error processing source ingestion on source ${source.name}", Some(ex))
        sourceLog.result = MetaInfo.Result.WithErrors
        ingestionLog.result = MetaInfo.Result.WithErrors
    }

    logger.info("SourceLog finished with id " + sourceLog.sourceLogId)
    metaLogManager.finishSourceLog(sourceLog)
    metaLogManager.finishIngestionLog(ingestionLog)
    ingestionLog
  }

  private def initProcessIngestionLoop(pendingSilver: List[DatasetLog]): Unit = {

    for (p <- pendingSilver) {

      logger.info("initializing DatasetLog")
      this.datasetLog = metaLogManager.initDatasetLog(sourceLog.sourceLogId, p.specificationId)
      datasetLog.bronzeDatasetLogId = Some(p.datasetLogId)
      datasetLog.reprocessed = p.reprocessed

      logger.info("DatasetLog initialized with id " + datasetLog.datasetLogId)
      logger.info("Init process ingestion for bronze DatasetLog " + p.datasetLogId)

      try {
        initProcessIngestion(p)
      } catch {
        case ex: Exception =>
          val sw = new StringWriter
          ex.printStackTrace(new PrintWriter(sw))
//          logger.error("" + sw.toString)
//          logger.error("" + ex.getClass.toString)
//          logger.error("" + ex.getMessage)
          logger.error(s"Error processing ingestion statistics on source ${source.name}.",Some(ex))
          datasetLog.resultMessage = Some(ex.getMessage)
          datasetLog.result = MetaInfo.Result.KO
          metaLogManager.finishDatasetLog(datasetLog)
          sourceLog.result = MetaInfo.Result.WithErrors
          ingestionLog.result = MetaInfo.Result.WithErrors
      }

      logger.info(" DatasetLog finished")
    }
  }

  private def initProcessIngestion(dl: DatasetLog): Unit = {

    var continue = true
    val specificationId = dl.specificationId

    logger.info("Getting specification from specification id " + specificationId)
    val dataset = source.datasets.find(_.specificationId.get == specificationId)

    if(dataset.isEmpty) {
      this.logger.error(s"Dataset with specificationId $specificationId not found in source ${source.name}",None)
      return
    }

    logger.info(" Getting required schema")
    val columns = dataset.get.schemaColumns.get.columns

    val primaryKeys = SchemaHelper.primaryKeys(dataset.get)

    val timeStampColumnName = SchemaHelper.timeStampColumn(dataset.get)

    val filePath = dl.outputPath.get

    logger.info("Reading dataframe from " + filePath)
    val df = datalakeManager.readBronze(filePath)


    val database = dataset.get.database
    val tableName = dataset.get.table
    val validationMode = dataset.get.validationMode
    val ingestionMode = dataset.get.ingestionMode
    val filePattern = dataset.get.fileInput.get.filePattern
    val partitionBy = dataset.get.partitionBy

    var validRows = spark.emptyDataFrame
    var invalidRowsDf = df
    var validRowsForSilver = spark.emptyDataFrame

    var qaErrorPath = ""
    var silverDatapath = ""
    var total_invalidRows: Long = df.count()
    var total_validRows: Long = 0

    val partitionPatterns = FileNameUtils.patterns(filePattern)
    val fileDate = PartitionHelper.getFileDateFromPartitions(filePath, partitionPatterns, partitionBy)

    logger.info("Validating number and presence of columns")
    val valid_cols = ValidationHelper.validateDFCols(df, columns)
    logger.info("Validation number and presence of columns: " + valid_cols)

    var valid_schema = false

    if (valid_cols) {

      logger.info("Casting columns to required types")
      val (validRows_, invalidRowsDf_) = ValidationHelper.validateDFRows(df, columns)

      validRows = validRows_
      invalidRowsDf = invalidRowsDf_

      total_invalidRows = invalidRowsDf.count()

      total_validRows = validRows.count()
      logger.info("Dataset has "+ total_validRows +" valid rows and " + total_invalidRows + " invalid rows")
      datasetLog.rowsOK = Some(total_validRows)
      datasetLog.rowsKO = Some(total_invalidRows)
      datasetLog.partition = Some(partitionBy)

      val requiredSchema = SchemaHelper.createStructSchema(columns)

      logger.info("Rechecking schema after casting columns")
      valid_schema = ValidationHelper.validateSchema(validRows, requiredSchema, dataset.get.name) && total_validRows > 0
      logger.info("Check schema after casting columns. Result: " + valid_schema)
      validRowsForSilver = validRows
    }

    val valid = valid_cols && valid_schema

    if (!valid || (total_invalidRows > 0 && (validationMode != ValidationTypes.Permissive ||
      !isNumWrongRowsUnderThreshold(dataset.get, total_invalidRows, df.count())))) {

      logger.error(s"Dataframe is not valid or error is bigger than threshold in dataset ${dataset.get.name}", None)
      datasetLog.result = MetaInfo.Result.KO
      sourceLog.result = MetaInfo.Result.WithErrors
      ingestionLog.result = MetaInfo.Result.WithErrors
      datasetLog.resultMessage = Some("Dataframe not valid for datasetLog: " + dl.datasetLogId + " and specificationId: " + specificationId)
      qaErrorPath = datalakeManager.writeQAError(invalidRowsDf, database, tableName, "", fileDate)
      continue = false

    } else if (total_invalidRows > 0) {

      logger.info("Dataframe is valid but has some errors")

      datasetLog.result = MetaInfo.Result.WithErrors
      sourceLog.result = MetaInfo.Result.WithErrors
      ingestionLog.result = MetaInfo.Result.WithErrors
      datasetLog.resultMessage = Some("Dataframe valid with some errors for datasetLog: " + dl.datasetLogId + " and specificationId: " + specificationId)

      logger.info("Writting invalid Dataframe through datalakeManager.writeQAError")
      qaErrorPath = datalakeManager.writeQAError(invalidRowsDf,  database, tableName, "", fileDate)

    }

    if (continue) {
      logger.info("Writting result dataframe in silver layer through datalakeManager.writeQAArchive")

      val unix_timestamp: Long = System.currentTimeMillis
      val dataLakeStorageDate = DateUtils.unixToDateTime(unix_timestamp)
      val validRowsWithSorageDate = validRowsForSilver.withColumn(SilverConstants.datalakeLoadDateFieldName, to_timestamp(lit(dataLakeStorageDate)
        , "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

      logger.info("Getting partitions info")
      val partitionPatterns = FileNameUtils.patterns(filePattern)

      logger.info("Adding partitions to dataframe")
      val dfToWriteSilver = PartitionHelper.addPartitionsByFileName(validRowsWithSorageDate, filePath, partitionPatterns, partitionBy)

      logger.info("Writting data to silver layer")
      try {
        silverDatapath = datalakeManager.writeSilver(ingestionMode,  database, tableName, dfToWriteSilver
          , partitionBy, primaryKeys, dataset.get.createDatabase, dataset.get.classification.toString
          , timeStampColumnName, datasetLog.reprocessed.get, dataset.get.getAllowPartitionChange, dataset.get.name)
        datasetLog.resultMessage = Some("Dataframe valid for datasetLog: " + dl.datasetLogId + " and specificationId: " + specificationId)
      } catch {
        case ex @ (_ : AnalysisException | _ : NewAndModifyException | _ : UnsupportedOperationException )=>
//          logger.error("" + ex.getClass.toString)
//          logger.error("" + ex.getMessage)
          logger.error(s"Error writting data to silver layer in dataset ${dataset.get.name}",Some(ex))
          datasetLog.resultMessage = Some(ex.getMessage)
          datasetLog.result = MetaInfo.Result.KO
          sourceLog.result = MetaInfo.Result.WithErrors
          ingestionLog.result = MetaInfo.Result.WithErrors
      }

      logger.info("Deleting bronze dataframe from " + filePath)
      datalakeManager.deleteBronzeData(filePath)
    }

    logger.info("Setting pending silver to false for datasetLog: " + dl.datasetLogId)
    metaLogManager.updatePendingSilverToFalse(dl)

    datasetLog.pendingSilver = Some(false)
    datasetLog.errorPath = Some(qaErrorPath)
    datasetLog.outputPath = Some(silverDatapath)
    datasetLog.archivePath = None
    metaLogManager.finishDatasetLog(datasetLog)

  }

  private def isNumWrongRowsUnderThreshold (dataset: Dataset, numWrongRows: Double, totalColumns: Long): Boolean = {
    if(dataset.permissiveThresholdType == PermissiveThresholdTypes.Percentage){
      val percentageWrongRows = (numWrongRows*100)/totalColumns
      percentageWrongRows <= dataset.permissiveThreshold
    }
    else{
      numWrongRows <= dataset.permissiveThreshold
    }
  }
}
