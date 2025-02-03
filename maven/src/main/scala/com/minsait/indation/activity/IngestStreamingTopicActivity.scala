package com.minsait.indation.activity

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.indation.Main.logger
import com.minsait.indation.activity.exceptions.IngestionException
import com.minsait.indation.bronze.validators.BronzeValidator
import com.minsait.indation.datalake.readers.LandingTopicReader
import com.minsait.indation.metadata.exceptions.SourceException
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}
import com.minsait.indation.metadata.{MetadataFilesManager, MetadataReader}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.transformers.SilverColumnTransformer
import com.minsait.indation.silver.validators.SilverValidator
import io.jvm.uuid.StaticUUID
import org.apache.spark.sql.DataFrame

class IngestStreamingTopicActivity(override val indationProperties: IndationProperties, val landingTopicReader: LandingTopicReader)
  extends IngestActivity {

  def this(indationProperties: IndationProperties) {
    this(indationProperties, new LandingTopicReader(indationProperties))
  }

  private val metadataManager: MetadataReader = new MetadataFilesManager(indationProperties)

  var startTimeMillis = System.currentTimeMillis
  var uuidIngestion = StaticUUID.randomString

  /** *
   * Realiza el proceso de validación de los datos y genera los dataframes con la información de bronze y silver, tanto válida como inválida.
   *
   * @param dataset
   * @param triggerId
   * @return La salida es una tupla 6 con los siguientes valores:
   *         1. Boolean que indica si la ejecución ha tenido éxito
   *            2. DataFrame con los registros válidos para la carga de bronze
   *            3. DataFrame con los registros inválidos para la carga de bronze
   *            4. DataFrame con los registros válidos para la carga de silver
   *            5. DataFrame con los registros inválidos para la carga de silver
   *            6. DataFrame principal de lectura
   *            7. String con la ruta del fichero AVRO
   */
  def generate(dataset: Dataset, triggerId: String): (Boolean, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, String) = {
    try {

      startTimeMillis = System.currentTimeMillis
      uuidIngestion = StaticUUID.randomString

      val source = metadataManager.sourceByName(dataset.sourceName) //.get
      if (source.isEmpty) {
        logger.error("Source " + dataset.sourceName + " not found.", None)
        throw new SourceException("Source " + dataset.sourceName + " not found.")
      }

      val (dataframe, avroFilePath) = landingTopicReader.readTopic(source.get, dataset)

      if (dataframe.isEmpty) {
        this.logger.info("Empty dataframe read from dataset " + dataset.name)
        return (false, null, null, null, null, null, null, null, null)
      }

      val (isValidFile, bronzeValidDf, bronzeInvalidDf) = BronzeValidator(dataset).validate(dataframe, dataset)

      if (!isValidFile) {
        val bronzeValidRows = bronzeValidDf.count()
        val bronzeInvalidRows = bronzeInvalidDf.count()
        this.schemaMismatchIngestion(avroFilePath, uuidIngestion, Some(dataset), bronzeValidRows, bronzeInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, None, None, None, None)
        return (false, null, null, null, null, null, null, null, null)
      }

      val (silverValidDf, silverInvalidDf) = SilverColumnTransformer(dataset).transform(bronzeValidDf, dataset)

      //Reglas de Calidad
      var qualityValidDf = silverValidDf
      var qualityInvalidDf = spark.emptyDataFrame

      if (dataset.qualityRules.isDefined && !silverValidDf.isEmpty) {

        val qualityDataframes = SilverValidator.validateQuality(dataset, silverValidDf)

        if (dataset.qualityRules.get.mode == RuleReject) {
          // las reglas que no se cumplen se incluyen a la hora de validar número de errores de ingesta
          qualityValidDf = qualityDataframes._1
          qualityInvalidDf = qualityDataframes._2
        }
        else if (dataset.qualityRules.get.mode == RuleWarning) {
          // las reglas que no se cumplen no influyen en la validación de errores de carga
          // no hacemos nada
        }
      }

      if (!SilverValidator.validate(dataset, qualityValidDf, silverInvalidDf, qualityInvalidDf)) {
        val bronzeValidRows = bronzeValidDf.count()
        val bronzeInvalidRows = bronzeInvalidDf.count()
        val silverValidRows = silverValidDf.count()
        val silverInvalidRows = silverInvalidDf.count()
        val qualityValidRows = qualityValidDf.count()
        val qualityInvalidRows = qualityInvalidDf.count()
        this.invalidIngestion(avroFilePath, uuidIngestion, Some(dataset), bronzeValidRows,
          bronzeInvalidRows, silverValidRows, silverInvalidRows, qualityValidRows, qualityInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, Some(silverValidDf), Some(silverInvalidDf),
          Some(qualityValidDf), Some(qualityInvalidDf))
        return (false, null, null, null, null, null, null, null, null)
      }

      //  Devolvemos datasets
      (true, bronzeValidDf, bronzeInvalidDf, silverValidDf, silverInvalidDf, qualityValidDf, qualityInvalidDf, dataframe, avroFilePath)

    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing streaming dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing streaming dataset ingest with name: ${dataset.name}", ex)
      }
    }
  }

  def execute(dataset: Dataset, triggerId: String): Boolean = {
    try {

      // Validamos y generamos datasets
      val result = generate(dataset, triggerId)

      if (result._1) {
        // Obtenemos valores para ingesta
        val bronzeValidDf = result._2
        val bronzeInvalidDf = result._3
        val silverValidDf = result._4
        val silverInvalidDf = result._5
        val qualityValidDf = result._6
        val qualityInvalidDf = result._7
        val dataframe = result._8
        var avroFilePath = result._9

        val bronzeValidRows = bronzeValidDf.count()
        val bronzeInvalidRows = bronzeInvalidDf.count()
        val silverValidRows = silverValidDf.count()
        val silverInvalidRows = silverInvalidDf.count()
        val qualityValidRows = qualityValidDf.count()
        val qualityInvalidRows = qualityInvalidDf.count()

        this.validIngestion(avroFilePath, uuidIngestion, Some(dataset), ReprocessTypes.None,
          bronzeInvalidDf, bronzeValidRows, bronzeInvalidRows, silverValidDf, silverInvalidDf, silverValidRows, silverInvalidRows, qualityValidRows, qualityInvalidRows, startTimeMillis, triggerId)

        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, Some(silverValidDf), Some(silverInvalidDf),
          Some(qualityValidDf), Some(qualityInvalidDf))

        true
      }
      else {
        false
      }
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing streaming dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing streaming dataset ingest with name: ${dataset.name}", ex)
      }
    }
  }
}
