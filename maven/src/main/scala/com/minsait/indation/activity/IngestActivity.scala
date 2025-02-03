package com.minsait.indation.activity

import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.EncryptUtils.{decryptFile, encryptFile}
import com.minsait.common.utils.FileNameUtils.getBronzeFileNameForTable
import com.minsait.indation.activity.statistics.models._
import com.minsait.indation.activity.statistics.{ActivityStatisticsHelper, ActivityStatisticsTableWriter, models}
import com.minsait.indation.datalake.LandingFileManager
import com.minsait.indation.datalake.writers.{BronzeFileWriter, SilverDeltaParquetTableWriter}
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.metadata.models.enums.IngestionTypes
import com.minsait.indation.metadata.{MetadataFilesManager, MetadataReader}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame

import java.io.File
import java.nio.file.Paths

trait IngestActivity extends SparkSessionWrapper with ConfigurationReader with LandingFileManager with BronzeFileWriter with Logging {

  private val metadataManager: MetadataReader = new MetadataFilesManager(indationProperties)

  private val statsWriter = new ActivityStatisticsTableWriter(indationProperties, this.metadataManager)

  protected def schemaMismatchIngestion(absolutePath   : String, uuidIngestion: String, dataset: Option[Dataset],
                                        bronzeValidRows: Long, bronzeInvalidRows: Long, startTimeMillis: Long, triggerId: String): Unit = {
    this.logger.error("Invalid File "+ absolutePath + " in dataset " + dataset.get.name,None)
    val schemaMismatchAbsolutePath = this.copyToSchemaMismatch(absolutePath, dataset.get)
    this.deleteFile(absolutePath)
    statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      ActivityResults.RejectedSchemaMismatch,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, Some(schemaMismatchAbsolutePath), None, None, None),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        None,
        None,
        None,
        None)
      ),
      None)
    )
  }

  protected def schemaMismatchTableIngestion(
                                              uuidIngestion: String,
                                              dataset: Option[Dataset],
                                              bronzeValidRows: Long,
                                              bronzeInvalidRows: Long,
                                              startTimeMillis: Long,
                                              triggerId: String): Unit = {
    this.logger.error("Invalid Table "+dataset.get.tableInput.get.getTable+ " in dataset " + dataset.get.name,None)
    val tableInput = dataset.get.tableInput.get
    val absolutePath = tableInput.getTablePath

    statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      ActivityResults.RejectedSchemaMismatch,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, Some(absolutePath), None, None, None),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        None,
        None,
        None,
        None)
      ),
      None)
    )
  }

  protected def schemaMismatchApiIngestion(
                                              uuidIngestion: String,
                                              source: Option[Source],
                                              dataset: Option[Dataset],
                                              bronzeValidRows: Long,
                                              bronzeInvalidRows: Long,
                                              startTimeMillis: Long,
                                              triggerId: String): Unit = {
    this.logger.error("Invalid schema of API response in dataset " + dataset.get.name,None)
    val apiInput = dataset.get.apiInput.get
    //TODO: ¿Incluir la ruta del source a la API?
    val url = if(apiInput.parameters.isDefined && apiInput.parameters.get.nonEmpty) {
      source.get.apiConnection.get.url + "/" + apiInput.endpoint + "?" + apiInput.parameters
    } else {
      source.get.apiConnection.get.url + "/" + apiInput.endpoint
    }


    statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      url,
      ActivityResults.RejectedSchemaMismatch,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, Some(url), None, None, None),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        None,
        None,
        None,
        None)
      ),
      None)
    )
  }

  protected def invalidIngestion(absolutePath: String, uuidIngestion: String, dataset: Option[Dataset], bronzeValidRows: Long, bronzeInvalidRows: Long,
                                 silverValidRows: Long, silverInvalidRows: Long, qualityValidRows: Long, qualityInvalidRows: Long, startTimeMillis: Long, triggerId: String): Unit = {
    logger.error("Silver validation Error dataset " + dataset.get.name,None)
    val invalidAbsolutePath = this.copyToInvalid(absolutePath, dataset.get)
    this.deleteFile(absolutePath)
    this.statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      ActivityResults.RejectedInvalid,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, None, Some(invalidAbsolutePath), None, None),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        Some(silverValidRows),
        Some(silverInvalidRows),
        Some(qualityValidRows),
        Some(qualityInvalidRows)
      )
      ),
      None)
    )
  }

  protected def invalidTableIngestion(uuidIngestion: String, dataset: Option[Dataset], bronzeValidRows: Long, bronzeInvalidRows: Long,
                                      silverValidRows: Long, silverInvalidRows: Long, qualityValidRows: Long, qualityInvalidRows: Long, startTimeMillis: Long, triggerId: String): Unit = {
    logger.error("Silver validation Error dataset " + dataset.get.name,None)
    val tableInput = dataset.get.tableInput.get
    val absolutePath = tableInput.getTablePath

    this.statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      ActivityResults.RejectedInvalid,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, None, Some(absolutePath), None, None),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        Some(silverValidRows),
        Some(silverInvalidRows),
        Some(qualityValidRows),
        Some(qualityInvalidRows)
      )
      ),
      None)
    )
  }

  protected def validIngestion(absolutePath: String, uuidIngestion: String, dataset: Option[Dataset], reprocessType: ReprocessTypes.ReprocessType, bronzeInvalidDf: DataFrame,
                               bronzeValidRows: Long, bronzeInvalidRows: Long, silverValidDf: DataFrame, silverInvalidDf: DataFrame,
                               silverValidRows: Long, silverInvalidRows: Long, qualityValidRows: Long, qualityInvalidRows: Long, startTimeMillis: Long, triggerId: String): Unit = {
    var bronzeAbsolutePath = this.copyCompressedToBronze(absolutePath, dataset.get)
    //val encryptRawFile = this.indationProperties.security.encryption.encryptRawFile.get
    //val sensitiveColumns =  SchemaHelper.sensitiveColumns(dataset.get)

//    if (encryptRawFile && sensitiveColumns.nonEmpty) {
//      val encryptedBronzeAbsolutePath = encryptFile(bronzeAbsolutePath)
//      this.deleteFile(bronzeAbsolutePath)
//      bronzeAbsolutePath = encryptedBronzeAbsolutePath
//    }

    val schemaMismathAbsolutePath = if (!bronzeInvalidDf.isEmpty) {
      Some(this.writeToSchemaMismatch(absolutePath, bronzeInvalidDf, dataset.get))
    } else {
      None
    }

    val invalidAbsolutePath = if (!silverInvalidDf.isEmpty) {
      Some(this.writeToInvalid(absolutePath, silverInvalidDf, dataset.get))
    } else {
      None
    }

    val silverWriteInfo = SilverDeltaParquetTableWriter.writeSilver(this.indationProperties,
      absolutePath,
      silverValidDf,
      dataset.get,
      uuidIngestion,
      reprocessType)

    this.deleteFile(absolutePath)
    statsWriter.receiveStatistics(ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      if (schemaMismathAbsolutePath.isDefined || invalidAbsolutePath.isDefined) {
        ActivityResults.Ingested_With_Errors
      }
      else {
        ActivityResults.Ingested_Without_Errors
      },
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, schemaMismathAbsolutePath, invalidAbsolutePath, Some(bronzeAbsolutePath),
        Some(silverWriteInfo._1.silverWritePath)),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        Some(silverValidRows),
        Some(silverInvalidRows),
        Some(qualityValidRows),
        Some(qualityInvalidRows)
      )
      ),
      Some(ActivityStatisticsHelper.activitySilverPersistente(dataset.get, silverWriteInfo._1)))
    )
  }

  protected def validJdbcTableIngestion(
                                     uuidIngestion: String,
                                     dataset: Option[Dataset],
                                     bronzeInvalidDf: DataFrame,
                                     bronzeValidRows: Long,
                                     bronzeInvalidRows: Long,
                                     silverValidDf: DataFrame,
                                     silverInvalidDf: DataFrame,
                                     silverValidRows: Long,
                                     silverInvalidRows: Long,
                                     qualityValidRows: Long,
                                     qualityInvalidRows: Long,
                                     startTimeMillis: Long,
                                     triggerId: String): Unit = {


    val tableInput = dataset.get.tableInput.get
    val absolutePath = tableInput.getTablePath

    logger.info("Escribiendo en bronze...")
    // Persistencia en capa Bronze
    var bronzeAbsolutePath =
      writeBronze(
        getBronzeFileNameForTable(dataset.get),
        silverValidDf,
        dataset.get,
        true
      )
    logger.info("Bronze OK")
    //val encryptRawFile = this.indationProperties.security.encryption.encryptRawFile.get
    //val sensitiveColumns =  SchemaHelper.sensitiveColumns(dataset.get)

//    if (encryptRawFile && sensitiveColumns.nonEmpty) {
//      val encryptedBronzeAbsolutePath = encryptFile(bronzeAbsolutePath)
//      this.deleteFile(bronzeAbsolutePath)
//      bronzeAbsolutePath = encryptedBronzeAbsolutePath
//    }
    val schemaMismathAbsolutePath = if (!bronzeInvalidDf.isEmpty) {
      Some(absolutePath)
    } else {
      None
    }

    val invalidAbsolutePath = if (!silverInvalidDf.isEmpty) {
      Some(absolutePath)
    } else {
      None
    }

    val reprocessType = {
      if (dataset.get.ingestionMode == IngestionTypes.FullSnapshot)
        ReprocessTypes.Full
      else
        ReprocessTypes.None
    }
    logger.info("Writing into Silver...")
    val silverWriteInfo = SilverDeltaParquetTableWriter.writeSilver(this.indationProperties,
      absolutePath,
      silverValidDf,
      dataset.get,
      uuidIngestion,
      reprocessType) //ReprocessTypes.Full)

    logger.info("Silver OK. Getting statistics...")
    statsWriter.receiveStatistics(ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      if (schemaMismathAbsolutePath.isDefined || invalidAbsolutePath.isDefined) {
        ActivityResults.Ingested_With_Errors
      }
      else {
        ActivityResults.Ingested_Without_Errors
      },
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, None, schemaMismathAbsolutePath, invalidAbsolutePath, Some(bronzeAbsolutePath),
        Some(silverWriteInfo._1.silverWritePath)),
      Some(ActivityRows(
        Some(bronzeValidRows),
        Some(bronzeInvalidRows),
        Some(silverValidRows),
        Some(silverInvalidRows),
        Some(qualityValidRows),
        Some(qualityInvalidRows)
      )
      ),
      Some(ActivityStatisticsHelper.activitySilverPersistente(dataset.get, silverWriteInfo._1)))
    )
  }

  protected def unpersistDataframes(fileDataframe: DataFrame, bronzeValidDataframe: DataFrame, bronzeInvalidDataframe: DataFrame,
                                    silverValidDataframe: Option[DataFrame], silverInvalidDataframe: Option[DataFrame],
                                    qualityValidRows: Option[DataFrame], qualityInvalidRows: Option[DataFrame]): Unit = {
    def safeUnpersist(dataFrame: DataFrame): Unit = {
      try {
        dataFrame.unpersist()
      } catch {
        case ex: Throwable => {
          logger.info("Could not unpersist dataframe")
        }
      }
    }
    logger.info("Unpersisting Dataframes from File")
    safeUnpersist(fileDataframe)
    logger.info("Unpersisting Dataframes from Bronze")
    safeUnpersist(bronzeValidDataframe)
    safeUnpersist(bronzeInvalidDataframe)
    logger.info("Unpersisting Dataframes from Silver")
    if (silverValidDataframe.isDefined) safeUnpersist(silverValidDataframe.get)
    if (silverInvalidDataframe.isDefined) safeUnpersist(silverInvalidDataframe.get)
    logger.info("Unpersisting Dataframes from Quality...")
    if(qualityValidRows.isDefined) qualityValidRows.get.unpersist()
    if(qualityInvalidRows.isDefined) qualityInvalidRows.get.unpersist()
  }
}
