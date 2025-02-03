package com.minsait.indation.activity

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.indation.activity.exceptions.IngestionException
import com.minsait.indation.bronze.validators.BronzeValidator
import com.minsait.indation.datalake.readers.LandingApiReader
import com.minsait.indation.metadata.exceptions.{DatasetException, SourceException}
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.metadata.models.enums.ColumnsTypes
import com.minsait.indation.metadata.models.enums.IngestionTypes.{Incremental, LastChanges}
import com.minsait.indation.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}
import com.minsait.indation.metadata.{MetadataFilesManager, MetadataReader}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.SchemaHelper
import com.minsait.indation.silver.transformers.SilverColumnTransformer
import com.minsait.indation.silver.validators.SilverValidator
import io.jvm.uuid.StaticUUID
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat


class IngestApiCallActivity(override val indationProperties: IndationProperties, val landingApiReader: LandingApiReader)
  extends IngestActivity {

  def this(indationProperties: IndationProperties) {
    this(indationProperties, new LandingApiReader(indationProperties))
  }

  private val metadataManager: MetadataReader = new MetadataFilesManager(indationProperties)

  var startTimeMillis: Long = System.currentTimeMillis
  var uuidIngestion: String = StaticUUID.randomString

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
   *            6. DataFrame con los registros válidos tras pasar las reglas de calidad
   *            7. DataFrame con los registros inválidos tras pasar las reglas de calidad
   *            8. DataFrame principal de lectura
   *            9. String con la ruta del fichero AVRO
   */
  def generate(dataset: Dataset, triggerId: String): (Boolean, DataFrame, DataFrame, DataFrame, DataFrame,DataFrame, DataFrame,DataFrame, String) = {
    try {

      startTimeMillis = System.currentTimeMillis
      uuidIngestion = StaticUUID.randomString

      // obtención de fuente
      val source = metadataManager.sourceByName(dataset.sourceName)
      if (source.isEmpty) {
        logger.error("Source " + dataset.sourceName + " not found.", None)
        throw new SourceException("Source " + dataset.sourceName + " not found.")
      }

      //Obtener valor de columna de carga incremental
      val lastValue: Option[String] = try{
        getLastValue(dataset, source, triggerId)
      } catch {
        case ex: DatasetException =>
          return (false,null,null,null,null,null, null, null, null)
      }

      // lectura de dataframe
      val (dataframe, avroFilePath) = landingApiReader.readApi(source.get, dataset, lastValue)

      // hacemos aquí el cache
      dataframe.cache()

      // validación de contenido
      if(dataframe.isEmpty) {
        this.logger.info("Empty dataframe read from dataset " + dataset.name)
        return (false,null,null,null,null,null, null, null, null)
      }

      // validación estándar de comprobación de nombres de los campos del esquema (BRONZE)
      val (isValidDataframe, bronzeValidDf, bronzeInvalidDf) = BronzeValidator(dataset).validate(indationProperties, avroFilePath, dataframe, dataset, Option(""))

      if (!isValidDataframe) {
        val bronzeValidRows = bronzeValidDf.count()
        val bronzeInvalidRows = bronzeInvalidDf.count()
        this.schemaMismatchIngestion(avroFilePath, uuidIngestion, Some(dataset), bronzeValidRows, bronzeInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, None, None,None,None)
        this.logger.error("Invalid Dataset " + dataset.name,None)
        return (false,null,null,null,null,null, null, null, null)
      }

      // Generación transformaciones de columnas de Silver
      val (silverValidDf, silverInvalidDf) = SilverColumnTransformer(dataset).transform(bronzeValidDf, dataset)

      //Reglas de Calidad
      var qualityValidDf = silverValidDf
      var qualityInvalidDf = spark.emptyDataFrame

      if (dataset.qualityRules.isDefined && !silverValidDf.isEmpty) {

        val qualityDataframes = SilverValidator.validateQuality(dataset, silverValidDf)

        if(dataset.qualityRules.get.mode == RuleReject) {
          // las reglas que no se cumplen se incluyen a la hora de validar número de errores de ingesta
          qualityValidDf = qualityDataframes._1
          qualityInvalidDf = qualityDataframes._2
        }
        else if(dataset.qualityRules.get.mode == RuleWarning){
          // las reglas que no se cumplen no influyen en la validación de errores de carga
          // no hacemos nada
        }

      }

      // Validación estándar de Silver
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
          Some(qualityValidDf),Some(qualityInvalidDf))
        return (false,null,null,null,null,null, null, null, null)
      }

      //  Devolvemos datasets
      (true, bronzeValidDf,bronzeInvalidDf, silverValidDf, silverInvalidDf, qualityValidDf, qualityInvalidDf, dataframe, avroFilePath)
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing API dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing API dataset ingest with name: ${dataset.name}",ex)
      }
    }
  }

  def execute(dataset: Dataset, triggerId: String): Boolean ={
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
        val avroFilePath = result._9

        val bronzeValidRows = bronzeValidDf.count()
        val bronzeInvalidRows = bronzeInvalidDf.count()
        val silverValidRows = silverValidDf.count()
        val silverInvalidRows = silverInvalidDf.count()
        val qualityValidRows = qualityValidDf.count()
        val qualityInvalidRows = qualityInvalidDf.count()

        // Ingesta
        this.validIngestion(avroFilePath, uuidIngestion, Some(dataset), ReprocessTypes.None,
          bronzeInvalidDf, bronzeValidRows, bronzeInvalidRows, silverValidDf, silverInvalidDf, silverValidRows, silverInvalidRows, qualityValidRows, qualityInvalidRows, startTimeMillis, triggerId)

        // Se hace unpersist por si se ha hecho cache
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, Some(silverValidDf), Some(silverInvalidDf),
          Some(qualityValidDf),Some(qualityInvalidDf))

        // Llegados aquí, ha ido bien
        true
      }
      else {
        false
      }

    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing API dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing API dataset ingest with name: ${dataset.name}",ex)
      }
    }
  }

  def getLastValue(dataset: Dataset, source: Option[Source], triggerId: String): Option[String] = {
    if (dataset.ingestionMode==Incremental || dataset.ingestionMode==LastChanges) {
      // Ruta de la tabla silver actual
      val silverTableName = s"${dataset.database}.${dataset.table}"

      //   Obtener columna timestamp del dataset (si no existe, generar error)
      val timeStampColumnName = SchemaHelper.timeStampColumn(dataset)
      if (timeStampColumnName == "") {
        this.schemaMismatchApiIngestion(uuidIngestion, source, Some(dataset), 0, 0, startTimeMillis, triggerId)

        this.logger.error("Invalid Dataset " + dataset.name + ", IsTimestamp column required for this type of Dataset.",None)
        throw new DatasetException("Invalid Dataset " + dataset.name + ", IsTimestamp column required for this type of Dataset.")
      }

      val timestampColumn = dataset.schemaColumns.get.columns.find(_.name.equals(timeStampColumnName))

      timestampColumn.get.typ match {
        case ColumnsTypes.Date | ColumnsTypes.DateTime => {
          val initialDate = "01/01/1900 00:00:00"

          // Format for input in UTC
          val dateFormat = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").withZone(DateTimeZone.UTC)
          // Parsing the date
          // La funcion toInstant devuelve la hora siempre en zona horaria UTC
          val timestamp = dateFormat.parseDateTime(initialDate).toInstant

          // Ejemplo del contenido de la variable: "1900-01-01T00:00:00.000Z"
          val defaultTimestamp = timestamp.toString

          if (spark.catalog.databaseExists(dataset.database) && spark.catalog.tableExists(silverTableName)) {

            //   Leer max columna timestamp de silver
            val dfMaxTimestamp =
              spark
                .read
                .table(silverTableName)
                .select(col(timeStampColumnName))
                .groupBy()
                .agg(
                  max( timeStampColumnName)
                    .alias("maxTimestamp")
                )
                .select(col("maxTimestamp"))

            // Si hay registro, aplicamos el filtro
            if (dfMaxTimestamp.count() == 1) {
              // La funcion toInstant devuelve la hora siempre en zona horaria UTC
              // Ejemplo del contenido de la variable: "1900-01-01T00:00:00Z"
              timestampColumn.get.typ match {
                case ColumnsTypes.DateTime =>
                  // La funcion toInstant devuelve la hora siempre en zona horaria UTC
                  // Ejemplo del contenido de la variable: "1900-01-01T00:00:00Z"
                  Some(dfMaxTimestamp.collect()(0).getTimestamp(0).toInstant.toString)
                case ColumnsTypes.Date =>
                  // Ejemplo del contenido de la variable: "1900-01-01"
                  Some(dfMaxTimestamp.collect()(0).getDate(0).toString)
              }
            } else {
              Some(defaultTimestamp)
            }
          } else {
            Some(defaultTimestamp)
          }
        }
        case _ =>
          this.schemaMismatchApiIngestion(uuidIngestion, source, Some(dataset), 0, 0, startTimeMillis, triggerId)
          this.logger.error("Invalid Dataset " + dataset.name + ", IsTimestamp column must be of type date or datetime.",None)
          throw new DatasetException("Invalid Dataset " + dataset.name + ", IsTimestamp column must be of type date or datetime.")
      }
    } else {
      None
    }
  }

}
