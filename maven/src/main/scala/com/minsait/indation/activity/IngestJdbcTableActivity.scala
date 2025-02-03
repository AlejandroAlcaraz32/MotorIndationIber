package com.minsait.indation.activity

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.indation.activity.exceptions.IngestionException
import com.minsait.indation.bronze.validators.BronzeValidator
import com.minsait.indation.datalake.readers.LandingJdbcTableReader
import com.minsait.indation.metadata.exceptions.SourceException
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.ColumnsTypes
import com.minsait.indation.metadata.models.enums.IngestionTypes.{Incremental, LastChanges}
import com.minsait.indation.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}
import com.minsait.indation.metadata.{MetadataFilesManager, MetadataReader}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.SchemaHelper
import com.minsait.indation.silver.transformers.SilverColumnTransformer
import com.minsait.indation.silver.validators.SilverValidator
import io.jvm.uuid.StaticUUID
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import scala.util.Try
import org.apache.spark.storage.StorageLevel


import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat


class IngestJdbcTableActivity(override val indationProperties: IndationProperties)
  extends IngestActivity {

  private val metadataManager: MetadataReader = new MetadataFilesManager(indationProperties)
  private val metadataFileReader = new MetadataFilesManager(indationProperties)

  var startTimeMillis = System.currentTimeMillis
  var uuidIngestion = StaticUUID.randomString

  /***
   * Realiza el proceso de validación de los datos y genera los dataframes con la información de bronze y silver, tanto válida como inválida.
   * @param dataset
   * @param triggerId
   * @return La salida es una tupla 6 con los siguientes valores:
   *         1. Boolean que indica si la ejecución ha tenido éxito
   *         2. DataFrame con los registros válidos para la carga de bronze
   *         3. DataFrame con los registros inválidos para la carga de bronze
   *         4. DataFrame con los registros válidos para la carga de silver
   *         5. DataFrame con los registros inválidos para la carga de silver
   *         6. DataFrame principal de lectura
   */
  def generate(dataset: Dataset, triggerId: String, sourcePathName: String): (Boolean, DataFrame, DataFrame, DataFrame, DataFrame,DataFrame, DataFrame,DataFrame) = {

    try {

      startTimeMillis = System.currentTimeMillis
      uuidIngestion = StaticUUID.randomString

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

      if (source.isEmpty){
        logger.error("Source " + dataset.sourceName + " not found.",None)
        throw new SourceException("Source " + dataset.sourceName + " not found.")
      }

      // lectura de dataframe
      //      var dataframe =  LandingJdbcTableReader.readTable(source.get,dataset,indationProperties, None)

      //////// INICIO CAMBIO onlyNewData
      // filtrado previo para casos de tipo incremental y last_changes
      //TODO: comprobar en DatasetValidator que haya columna timestamp en caso de ser incremental o last changes
      val timeStampColumnName = SchemaHelper.timeStampColumn(dataset)
      var dataframe = if ((dataset.ingestionMode==Incremental && !timeStampColumnName.equals("")) || dataset.ingestionMode==LastChanges) {
        // Ruta de la tabla silver actual
        val silverTableName = s"${dataset.database}.${dataset.table}"
        val tableExists = Try {
          spark.read.table(silverTableName).take(1)
          true
        }.getOrElse(false)

        if (tableExists) {
          val timeStampColumnAlias = SchemaHelper.getColumnAlias(dataset, timeStampColumnName).getOrElse(timeStampColumnName)

          //  Leer max columna timestamp de silver. Se tiene en cuenta si el campo isTimestamp tiene alias en Silver
          val dfMaxTimestamp =
            spark
              .read
              .table(silverTableName)
              .select(col(timeStampColumnAlias))
              .groupBy()
              .agg(
                max(timeStampColumnAlias)
                  .alias("maxTimestamp")
              )
              .select(col("maxTimestamp"))

          val timestampValue = dfMaxTimestamp.collect()(0).getTimestamp(0)
          // Si hay registro (la tabla no esta vacía), obtenemos el max timestamp
          if (timestampValue != null) {

            if (dataset.tableInput.get.onlyNewData.getOrElse(false)){
              val dateFormat = new SimpleDateFormat(dataset.tableInput.get.datePattern.get)
              val timestampValueString = dateFormat.format(timestampValue)

              LandingJdbcTableReader.readTable(source.get,dataset,indationProperties, Some(timestampValueString))
            }
            else {
              val dataframe = LandingJdbcTableReader.readTable(source.get,dataset,indationProperties, None)

              //   Establecer en el DataFrame el filtrado "where columna_timestamp > valor_max_timestap_leido"
              dataframe
                .where(col(timeStampColumnName) > timestampValue)
            }
          } else {
            LandingJdbcTableReader.readTable(source.get,dataset,indationProperties, None)
          }
        } else {
          LandingJdbcTableReader.readTable(source.get,dataset,indationProperties, None)
        }
      }
      else {
        LandingJdbcTableReader.readTable(source.get,dataset,indationProperties, None)
      }
      //////// FIN CAMBIO onlyNewData

      // Limpieza previa de columnas no deseadas
      val requiredSchema = SchemaHelper.structTypeSchema(dataset)
      dataframe.columns.foreach(c => {
        val col = requiredSchema.filter(r => r.name==c)
        if (col.isEmpty || col.length==0){
          logger.info(s"Non required jdbc column '$c' ignored in table '${dataset.tableInput.get.getTable}'")
          dataframe=dataframe.drop(c)
        }
      })

      // hacemos aquí el cache
      dataframe.cache()

      // validación de contenido
      if(dataframe.isEmpty) {
        this.logger.info("Empty dataframe read from dataset " + dataset.name)
        //      return
        //TODO: Validar qué hacemos cuando está vacío el dataset. De momento, lo dejamos continuar para generar estadísticas.
        // otra opción es registrar aquí los resultados con 0 registros y evitar que trabaje innecesariamente
      }

      // validación estándar de comprobación de esquema y poco más (BRONZE)
      val (isValidDataframe, bronzeValidDf, bronzeInvalidDf) = BronzeValidator(dataset).validate( dataframe, dataset)

      bronzeValidDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
      if (!isValidDataframe) {
        val bronzeValidRows = bronzeValidDf.count()
        val bronzeInvalidRows = bronzeInvalidDf.count()
        this.schemaMismatchTableIngestion(uuidIngestion, Some(dataset), bronzeValidRows, bronzeInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, None, None,None,None)
        this.logger.error("Invalid Dataset " + dataset.name,None)
        return (false,null,null,null,null,null, null, null)
      }

      // Generación transformaciones de columnas de Silver
      val (silverValidDf, silverInvalidDf) = SilverColumnTransformer(dataset).transform(bronzeValidDf, dataset)
      silverValidDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
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
        this.invalidTableIngestion(uuidIngestion, Some(dataset), bronzeValidRows,
          bronzeInvalidRows, silverValidRows, silverInvalidRows, qualityValidRows, qualityInvalidRows, startTimeMillis, triggerId)
        this.unpersistDataframes(dataframe, bronzeValidDf, bronzeInvalidDf, Some(silverValidDf), Some(silverInvalidDf),
          Some(qualityValidDf),Some(qualityInvalidDf))
        return (false,null,null,null,null,null, null, null)
      }

      //  Devolvemos datasets
      (true, bronzeValidDf,bronzeInvalidDf, silverValidDf, silverInvalidDf, qualityValidDf, qualityInvalidDf, dataframe )
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Error executing jdbc dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing jdbc dataset ingest with name: ${dataset.name}",ex)
      }
    }

  }

  def execute(dataset: Dataset, triggerId: String, sourcePathName: String): Boolean = {

    try {

      // Validamos y generamos datasets
      val result = generate(dataset,triggerId, sourcePathName)
      logger.info("Table was read and dataframes were created correctly!")

      if (result._1) {
        // Obtenemos valores para ingesta
        val bronzeValidDf = result._2
        val bronzeInvalidDf = result._3
        val silverValidDf = result._4
        val silverInvalidDf = result._5
        val qualityValidDf = result._6
        val qualityInvalidDf = result._7
        val dataframe = result._8

        val bronzeValidRows = if (bronzeValidDf.isEmpty) 0 else bronzeValidDf.count()
        val bronzeInvalidRows = if (bronzeInvalidDf.isEmpty) 0 else bronzeInvalidDf.count()
        val silverValidRows = if (silverValidDf.isEmpty) 0 else silverValidDf.count()
        val silverInvalidRows = if (silverInvalidDf.isEmpty) 0 else silverInvalidDf.count()
        val qualityValidRows = if (qualityValidDf.isEmpty) 0 else qualityValidDf.count()
        val qualityInvalidRows = if (qualityInvalidDf.isEmpty) 0 else qualityInvalidDf.count()


        logger.info("Writing into bronze and silver...")
        // Ingesta
        this.validJdbcTableIngestion(uuidIngestion, Some(dataset),
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
        logger.error(s"Error executing jdbc dataset ingest with name: ${dataset.name}", Some(ex))
        throw new IngestionException(s"Error executing jdbc dataset ingest with name: ${dataset.name}",ex)
      }
    }

  }
}
