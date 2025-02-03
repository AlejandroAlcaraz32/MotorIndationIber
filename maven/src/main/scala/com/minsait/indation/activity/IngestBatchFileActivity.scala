package com.minsait.indation.activity

import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.EncryptUtils.{decryptFile, encryptFile}
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.indation.activity.exceptions.IngestionException
import com.minsait.indation.activity.statistics.models.ActivityResults._
import com.minsait.indation.activity.statistics.models.{ActivityRows, ActivityStatistics, ActivityTrigger, ActivityTriggerTypes}
import com.minsait.indation.activity.statistics.{ActivityStatisticsHelper, ActivityStatisticsTableWriter, models}
import com.minsait.indation.bronze.validators.BronzeValidator
import com.minsait.indation.datalake.writers.{BronzeFileWriter, SilverDeltaParquetTableWriter}
import com.minsait.indation.datalake.{DatalakeManager, LandingFileManager}
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.IngestionTypes
import com.minsait.indation.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}
import com.minsait.indation.metadata.{MetadataFilesManager, MetadataReader}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.SchemaHelper
import com.minsait.indation.silver.transformers.SilverColumnTransformer
import com.minsait.indation.silver.validators.SilverValidator
import io.jvm.uuid.StaticUUID
import org.apache.spark.sql.DataFrame
import org.joda.time.{DateTime, DateTimeZone}
import java.io.File
import java.nio.file.Paths
import scala.util.{Failure, Success, Try}
import org.apache.spark.storage.StorageLevel

class IngestBatchFileActivity(override val indationProperties: IndationProperties)
  extends SparkSessionWrapper with ConfigurationReader with LandingFileManager with BronzeFileWriter with Logging {

  private val fs: FSCommands = FSCommandsFactory.getFSCommands(indationProperties.environment)

  private val metadataManager: MetadataReader = new MetadataFilesManager(indationProperties)

  private val metadataFileReader = new MetadataFilesManager(indationProperties)

  private val statsWriter = new ActivityStatisticsTableWriter(indationProperties, this.metadataManager)

  def executeWithFile(file: String, triggerId: String, datasetDF: Option[Dataset] = None): Boolean = {

    try {

      val startTimeMillis = System.currentTimeMillis
 
      this.logger.info("IngestBatchFileActivity: " + file)
      val absolutePath = this.absoluteFilePath(file)
      this.logger.info("IngestBatchFileActivity absolute path: " + absolutePath)
   
      val uuidIngestion = StaticUUID.randomString

      if (!this.fs.exists(absolutePath)) {
        this.logger.error("Error reading file from landing pending: File not found " + absolutePath, None)
        //      return
        // Es un error propagable, aún no ha iniciado la gestión y no genera estadística
        throw new NoSuchElementException("Error reading file from landing pending: File not found " + absolutePath)
      }

      this.fs.waitForFinishedCopy(absolutePath)

      logger.info(s"Finding dataset for $absolutePath.")
      val fileName = absolutePath.split("/").last
      val dataset = datasetDF match {
        case Some(d) => datasetDF
        case _ => this.metadataManager.datasetForFile(fileName)
      }

      if (dataset.isEmpty) {
        logger.info(s"Could not find dataset for $absolutePath. Moving file to landing/unknown.")
        logger.info(s"An error will be thrown by this execution.")
        unknownFileIngestion(triggerId, startTimeMillis, absolutePath, uuidIngestion)
        return false
      }

      val dataframe = this.readFileLandingPending(absolutePath, dataset.get) match {
        case Success(df) => df
        case Failure(e) => {
          this.logger.error(s"Error reading file $absolutePath from landing pending in dataset ${dataset.get.name}: ", Some(e))

          this.corruptedFileIngestion(absolutePath, uuidIngestion, dataset, e, startTimeMillis, triggerId)
          return false
        }
      }

      Try(dataframe.isEmpty) match {
        case Success(_) => this.logger.info("File is not corrupted: " + fileName)
        case Failure(e) => {
          this.corruptedFileIngestion(absolutePath, uuidIngestion, dataset, e, startTimeMillis, triggerId)

          return false
        }
      }

      val (isValidFile, bronzeValidDf, bronzeInvalidDf) = BronzeValidator(dataset.get)
        .validate(indationProperties, absolutePath, dataframe, dataset.get, Option(""))
      
      bronzeValidDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val bronzeValidRows = bronzeValidDf.count()
      val bronzeInvalidRows = bronzeInvalidDf.count()

      if (!isValidFile) {
        this.schemaMismatchIngestion(absolutePath, uuidIngestion, dataset, bronzeValidRows, bronzeInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, None, None,None,None)
        return false
      }

      val (silverValidDf, silverInvalidDf) = SilverColumnTransformer(dataset.get).transform(bronzeValidDf, dataset.get)

      // Cache the silver valid DF that will be written
      silverValidDf.persist(StorageLevel.MEMORY_AND_DISK_SER)

      val silverValidRows = silverValidDf.count()
      val silverInvalidRows = silverInvalidDf.count()

      //Reglas de Calidad
      var qualityValidDf = silverValidDf
      var qualityInvalidDf = spark.emptyDataFrame

      if (dataset.get.qualityRules.isDefined && !silverValidDf.isEmpty) {

        val qualityDataframes = SilverValidator.validateQuality(dataset.get, silverValidDf)

        if(dataset.get.qualityRules.get.mode == RuleReject) {
          // las reglas que no se cumplen se incluyen a la hora de validar número de errores de ingesta
          qualityValidDf = qualityDataframes._1
          qualityInvalidDf = qualityDataframes._2
        }
        else if(dataset.get.qualityRules.get.mode == RuleWarning){
          // las reglas que no se cumplen no influyen en la validación de errores de carga
          // no hacemos nada
        }
      }
      val qualityValidRows = qualityValidDf.count()
      val qualityInvalidRows = qualityInvalidDf.count()

      if (!SilverValidator.validate(dataset.get, qualityValidDf, silverInvalidDf, qualityInvalidDf)) {
        this.invalidIngestion(absolutePath, uuidIngestion, dataset, bronzeValidRows,
          bronzeInvalidRows, silverValidRows, silverInvalidRows, qualityValidRows, qualityInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, Some(silverValidDf), Some(silverInvalidDf),
          Some(qualityValidDf),Some(qualityInvalidDf))
        return false
      }

      logger.info("Valid ingestion!")

      this.validIngestion(absolutePath, uuidIngestion, dataset, bronzeValidDf,
        bronzeInvalidDf, bronzeValidRows, bronzeInvalidRows, silverValidDf, silverInvalidDf, Some(qualityValidDf),Some(qualityInvalidDf), dataframe, silverValidRows, silverInvalidRows, qualityValidRows, qualityInvalidRows, startTimeMillis, triggerId)


      true
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing file ingest with filename: $file", Some(ex))
        throw new IngestionException(s"Error executing file ingest with filename: $file", ex)
      }
    }
  }

  def executeWithDataset(dataset: Dataset, triggerId: String, sourcePathName: String): Boolean = {
    
    try {
      // obtención de fuente

      val source = if (sourcePathName.endsWith(".json")) {

          metadataFileReader.sourceByPath(sourcePathName)
        } else {
          try {
                // Attempt to construct the path from sourceName

                val constructedPath = s"$sourcePathName/source-${dataset.sourceName}.json"
                logger.info("Attempting to load source from constructed path: " + constructedPath)
                // Try to load the dataset using the constructed path
                metadataFileReader.sourceByPath(constructedPath)
              } catch {
                case _: Exception => 
                  // If it fails, fall back to sourceByName
                  logger.info("Could not find the source in constructed path.")
                  throw new NoSuchElementException("Esta versión del motor ya no soporta la búsqueda recursiva de 'sources'.\nLos sources deben empezar por 'source-' y el nombre del origen (source definido dentro del json del dataset).\nRevisa que el source exista en la carpeta correcta.")
                  //metadataManager.sourceByName(dataset.sourceName)
              }
        }

      if (source.isEmpty) {
        throw new NoSuchElementException(s"Source ${dataset.sourceName} not found")
      }

      if (dataset.isActive(DateTime.now(DateTimeZone.UTC).getMillis)){
        // Leer todos los ficheros de pending

        val datalakeManager = new DatalakeManager(indationProperties)

        val files = datalakeManager.getPendingFiles(source.get)

        // Recorrer cada fichero leído y, si cumple el patrón del dataset, invocar execute file
        var errorDetected = false
        var msg = ""
        files.foreach(file => {
          try {
            val fileName = file.replace("\\", "/").split("/").last

            // val ds = metadataManager.datasetForFile(fileName)
            if (dataset.matchesFileName(fileName)) {

              executeWithFile(file.replace("\\", "/").replace(this.indationProperties.landing.basePath, ""), triggerId, Some(dataset))

            }
          }
          catch {
            case ex: Throwable => {
              errorDetected = true

              logger.error(s"Error ingesting file $file", Some(ex))
              msg += s"Error ingesting file $file\n"
            }
          }
        })
        if (errorDetected) {
          throw new IngestionException("Error executing some ingestion files.\n" + msg)
        }
      } else {
        throw new IngestionException("Dataset is not active: " + dataset.name)
      }

      // Si llegamos hasta aquí, la ejecución ha tenido éxito
      true
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing file dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing file dataset ingest with name: ${dataset.name}", ex)
      }
    }

  }

  private def unpersistDataframes(fileDataframe: DataFrame, bronzeValidDataframe: DataFrame, bronzeInvalidDataframe: DataFrame,
                                  silverValidDataframe: Option[DataFrame], silverInvalidDataframe: Option[DataFrame],
                                  qualityValidRows: Option[DataFrame], qualityInvalidRows: Option[DataFrame]): Unit = {
    def safeUnpersist(dataFrame: DataFrame): Unit = {
      try {
        dataFrame.unpersist()
      } catch {
        case ex: Throwable => {
          logger.info(s"Could not unpersist dataframe: ${ex.getMessage}")
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
  }

  private def unknownFileIngestion(triggerId: String, startTimeMillis: Long, absolutePath: String, uuidIngestion: String): Unit = {
    val unknownAbsolutePath = this.copyToUnknown(absolutePath)
    this.deleteFile(absolutePath)
    statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      None,
      absolutePath,
      RejectedUnknown,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(None, Some(unknownAbsolutePath), None, None, None, None, None),
      None,
      None)
    )
  }

  private def validIngestion(absolutePath: String, uuidIngestion: String, dataset: Option[Dataset], bronzeValidDf: DataFrame, bronzeInvalidDf: DataFrame,
                             bronzeValidRows: Long, bronzeInvalidRows: Long, silverValidDf: DataFrame, silverInvalidDf: DataFrame,
                             qualityValidDf: Option[DataFrame], qualityInvalidDf: Option[DataFrame], fileDataframe: DataFrame,
                             silverValidRows: Long, silverInvalidRows: Long, qualityValidRows: Long, qualityInvalidRows: Long,
                             startTimeMillis: Long, triggerId: String): Unit = {
             
    var bronzeAbsolutePath = this.copyCompressedToBronze(absolutePath, dataset.get)

//    val encryptRawFile = this.indationProperties.security.encryption.encryptRawFile.get
//    val sensitiveColumns =  SchemaHelper.sensitiveColumns(dataset.get)
//
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

    val reprocessType = {
      if (dataset.get.ingestionMode == IngestionTypes.FullSnapshot)
        ReprocessTypes.Full
      else
        ReprocessTypes.None
    }

    val silverWriteInfo = SilverDeltaParquetTableWriter.writeSilver(this.indationProperties,
      absolutePath,
      silverValidDf,
      dataset.get,
      uuidIngestion,
      reprocessType) //ReprocessTypes.Full)

    logger.info("Ingestion succeed: Writing Statistics...")



    statsWriter.receiveStatistics(ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      if (schemaMismathAbsolutePath.isDefined || invalidAbsolutePath.isDefined) {
        Ingested_With_Errors
      }
      else {
        Ingested_Without_Errors
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

    logger.info("Unpersisting dataframes...")
    this.unpersistDataframes(fileDataframe, bronzeValidDf, bronzeInvalidDf, Some(silverValidDf), Some(silverInvalidDf),
        qualityValidDf,qualityInvalidDf)

    this.deleteFile(absolutePath)
  }

  private def corruptedFileIngestion(absolutePath: String, uuidIngestion: String, dataset: Option[Dataset], e: Throwable, startTimeMillis: Long,
                                     triggerId: String): Unit = {
    this.logger.error(s"Error reading file $absolutePath from landing pending in dataset ${dataset.get.name}", Some(e))
    val corruptedAbsolutePath = this.copyToCorrupted(absolutePath, dataset.get)
    this.deleteFile(absolutePath)
    statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      RejectedCorrupted,
      ActivityStatisticsHelper.activityExecution(startTimeMillis, System.currentTimeMillis()),
      ActivityStatisticsHelper.activityOutputPaths(dataset, None, Some(corruptedAbsolutePath), None, None, None, None),
      None,
      None)
    )
  }

  private def schemaMismatchIngestion(absolutePath: String, uuidIngestion: String, dataset: Option[Dataset],
                                      bronzeValidRows: Long, bronzeInvalidRows: Long, startTimeMillis: Long, triggerId: String): Unit = {
    this.logger.error("Invalid File " + absolutePath + " in dataset " + dataset.get.name, None)
    val schemaMismatchAbsolutePath = this.copyToSchemaMismatch(absolutePath, dataset.get)
    this.deleteFile(absolutePath)
    statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      RejectedSchemaMismatch,
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

  private def invalidIngestion(absolutePath: String, uuidIngestion: String, dataset: Option[Dataset], bronzeValidRows: Long, bronzeInvalidRows: Long,
                               silverValidRows: Long, silverInvalidRows: Long, qualityValidRows: Long, qualityInvalidRows: Long, startTimeMillis: Long, triggerId: String): Unit = {
    logger.error("Silver validation Error in dataset " + dataset.get.name, None)
    val invalidAbsolutePath = this.copyToInvalid(absolutePath, dataset.get)
    this.deleteFile(absolutePath)
    this.statsWriter.receiveStatistics(models.ActivityStatistics(
      uuidIngestion,
      ActivityTrigger(ActivityTriggerTypes.Adf, triggerId),
      None,
      Some(ActivityStatisticsHelper.activityDataset(dataset.get)),
      absolutePath,
      RejectedInvalid,
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
}
