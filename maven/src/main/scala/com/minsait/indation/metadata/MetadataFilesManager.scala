package com.minsait.indation.metadata

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.fs.FSCommandsFactory
import com.minsait.indation.metadata.models.{Dataset, Source}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.expr
import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import scala.collection.JavaConverters._


class MetadataFilesManager(private val indationProperties: IndationProperties) extends MetadataReader
  with SparkSessionWrapper with Logging {

  lazy val sources: List[Source] = readSources

  private val fs = FSCommandsFactory.getFSCommands(indationProperties.environment)

  def validateMetadata( jsonPath:String):(List[String],List[String])={
    val directoriesSources = fs.directoriesWithSources(jsonPath)
      .map(_ + "source*.json")
    val directoriesWithDatasets = fs.directoriesWithDatasets(jsonPath)
      .map(_ + "dataset*.json")
    (validateSources(directoriesSources),validateDatasets(directoriesWithDatasets))
  }

  def readSources: List[Source] = {

    throw new NoSuchElementException("Esta versión del motor ya no soporta la búsqueda recursiva de 'sources'.\nLos sources deben empezar por 'source-' y el nombre del origen (source definido dentro del json del dataset).\nRevisa que el source exista en la carpeta correcta.")

    val directoriesSources = fs.directoriesWithSources(indationProperties.metadata.basePathIngestion)
      .map(_ + "source*.json")

    var ficheros: List[String] = List()

    for (w <- 0 to directoriesSources.length - 1) {
      val path = directoriesSources(w).replace("source*.json","")
      val ficherosDirectorio: List[String] = getListOfFiles(path, "source.*\\.json$")
      ficheros = ficheros ++ ficherosDirectorio
    }

    import spark.implicits._

    //recogemos las rutas de json que son erróneas y las filtramos

    val badDirectories = validateSources(directoriesSources)

    val directoriostotal = ficheros diff badDirectories

    import spark.implicits._

    spark.read
      .schema(Encoders.product[Source].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)
      .as[Source].collectAsList.asScala.toList
  }

  lazy val datasets: List[Dataset] = readDatasets

  def readDatasets: List[Dataset] = {

    throw new NoSuchElementException("Esta versión del motor ya no soporta la búsqueda recursiva de 'datasets'.\nLos datasets deben empezar por 'dataset-' y contener el nombre de la carpeta source a continuación.\n{sourceFolder}/dataset-{sourceFolder}-{datasetName}/dataset-{sourceFolder}-{datasetName}.json")
    logger.info("Searching datasets... This might take a while...")
    val directoriesWithDatasets = fs.directoriesWithDatasets(indationProperties.metadata.basePathIngestion)
      .map(_ + "dataset*.json")

    var ficheros: List[String] = List()

    for (w <- 0 to directoriesWithDatasets.length - 1) {
      val path = directoriesWithDatasets(w).replace("dataset*.json","")
      val ficherosDirectorio: List[String] = getListOfFiles(path, "dataset.*\\.json$")
      ficheros = ficheros ++ ficherosDirectorio
    }
    logger.info(s"Read ${ficheros.length} datasets.")

    import spark.implicits._

    //recogemos las rutas de json que son erróneas y las filtramos

    val badDirectories = validateDatasets(directoriesWithDatasets)

    val directoriostotal = ficheros diff badDirectories

    val temp = spark.read
      .schema(Encoders.product[Dataset].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)


    // Con esta línea eliminamos los datasets mal leídos
    var outputValues =
      temp
        .where("version is not null")
        .as[Dataset].collectAsList.asScala.toList

    val badTypeValues = outputValues.filter(d =>
      (d.schemaColumns.isDefined && d.schemaColumns.get.columns.filter(c => c.typ == null).length > 0)
    )

    // comprobamos datasets con columnas mal formadas y los eliminamos
    if (badTypeValues.length > 0) {
      // primero eliminamos
      outputValues = outputValues diff badTypeValues;

      // luego informamos
      badTypeValues.foreach(d => {
        val badColumns = d.schemaColumns.get.columns.filter(c => c.typ == null)
        badColumns.foreach(c => {
          logger.error(s"Error parsing dataset ${d.name}. The column ${c.name} has a wrong type defined", None)
        })
      })
    }

    // Comprobamos si se han eliminado datasets de la lectura
    if (temp.collect.length != outputValues.length) {
      val temp2 = spark.read
        .option("multiline", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .json(directoriesWithDatasets: _*)

      val notLoadedDF = temp2.alias("t2").join(temp.alias("t1"), expr("t2.name=t1.name"), "leftanti")

      logger.error(s"Error parsing datasets, ${notLoadedDF.count()} files not loaded.\n${notLoadedDF.collect.map(r => "Bad JSON detected: \n" + r.toString()).mkString("\n\n")}", None)
    }

    outputValues
  }

  def datasetByPath(path: String): Option[Dataset] = {

    import spark.implicits._
    logger.info("Getting Dataset by Path:" + path)

    
    val temp = spark.read
      .schema(Encoders.product[Dataset].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(this.indationProperties.metadata.basePathIngestion+path)

    temp.as[Dataset].collectAsList.asScala.toList.headOption
  }
  
  def sourceByPath(path: String): Option[Source] = {
    import spark.implicits._
    logger.info("Getting Source by Path:" + path)
    val temp = spark.read
      .schema(Encoders.product[Source].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(this.indationProperties.metadata.basePathIngestion+path)
      
    temp.as[Source].collectAsList.asScala.toList.headOption
  }

  def validateDatasets(directoriesWithDatasets: List[String]): List[String] = {

    val jsonSchemaPath = this.getClass.getResource("/jsonSchemaFiles/schema-dataset.json")
    val schemaJsonString = jsonSchemaPath.openStream()//jsonSchemaPath.getFile
    val schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaJsonString)

    //val directoriesWithDatasets = fs.directoriesWithDatasets(indationProperties.metadata.basePathIngestion)
      //.map(_ + "dataset*.json")

    var outputValues: List[String] = List()

    for (w <- 0 to directoriesWithDatasets.length - 1) {
//      val lastOne = directoriesWithDatasets(w).split("/")(0)
//      val ficheros: List[String] = getListOfFiles(lastOne, "dataset.*\\.json$")
      val path = directoriesWithDatasets(w).replace("dataset*.json","")
      val ficheros: List[String] = getListOfFiles(path, "dataset.*\\.json$")

      for (z <- 0 to ficheros.length - 1) {
        val json_content = fs.fileContent(ficheros(z)).mkString
        try {
          val parsedJson = new ObjectMapper().readTree(json_content)
          val validation = schema.validate(parsedJson).asScala
          if (validation.nonEmpty) {
            validation.foreach(msg => this.logger.error(msg.getMessage + " -> " + ficheros(z), None))
            outputValues = outputValues ++ List(ficheros(z))
          }
        } catch {
          case _: Throwable =>
            logger.error("Error parsing JSON -> " + ficheros(z), None)
            outputValues = outputValues ++ List(ficheros(z))
        }
      }
    }
    outputValues

  }

  /**
   * Devuelve la lista de ficheros de una carpeta que cumplen con la expresión regular pattern
   * @param dir: carpeta de búsqueda
   * @param pattern: expresión regular a buscar
   * @return
   */
  def getListOfFiles(dir: String, pattern: String): List[String] = {
    val directory = dir.replace("\\","/")
    val files = fs.ls(directory)
    val jsonFiles = files.filter(_.replace("\\","/").replace(directory,"").matches(pattern))
    jsonFiles.toList
  }

  def validateSources(directoriesSources:List[String]): List[String] = {

    val jsonSchemaPath = this.getClass.getResource("/jsonSchemaFiles/schema-source.json")
    val schemaJsonString = jsonSchemaPath.openStream()//jsonSchemaPath.getFile
    val schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaJsonString)

    //    val directoriesSources = fs.directoriesWithSources(indationProperties.metadata.basePathIngestion)
//      .map(_ + "source*.json")
    var outputValues: List[String] = List()

    for (w <- 0 to directoriesSources.length - 1) {
//      val lastOne = directoriesSources(w).split("/")(0)
//      val ficheros: List[String] = getListOfFiles(lastOne, "source.*\\.json$")
      val path = directoriesSources(w).replace("source*.json","")
      val ficheros: List[String] = getListOfFiles(path, "source.*\\.json$")

      for (z <- 0 to ficheros.length - 1) {
        val json_content = fs.fileContent(ficheros(z)).mkString
        try {
          val parsedJson = new ObjectMapper().readTree(json_content)
          val validation = schema.validate(parsedJson).asScala
          if (validation.nonEmpty) {
            validation.foreach(msg => this.logger.error(msg.getMessage + " -> " + ficheros(z), None))
            outputValues = outputValues ++ List(ficheros(z))
          }
        } catch {
          case _: Throwable =>
            logger.error("Error parsing JSON -> " + ficheros(z), None)
            outputValues = outputValues ++ List(ficheros(z))
        }
      }
    }
    outputValues
  }
}
