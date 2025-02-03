package com.minsait.metagold.metadata

import com.fasterxml.jackson.databind.ObjectMapper
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.utils.fs.FSCommandsFactory
import com.minsait.common.utils.fs.LocalFSCommands.{activityString, sqlconnectionString, transformationsString}
import com.minsait.metagold.logging.Logging
import com.minsait.metagold.metadata.exceptions.NonUniqueDatasetException
import com.minsait.metagold.metadata.models.{Activity, SQLConnection, Transformation}
import com.minsait.common.spark.SparkSessionWrapper
import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import org.apache.spark.sql.Encoders

import scala.collection.JavaConverters._


class MetadataFilesManager(private val metagoldProperties: IndationProperties) extends SparkSessionWrapper with Logging {

  private val fs = FSCommandsFactory.getFSCommands(metagoldProperties.environment)

  lazy val sqlConnections: List[SQLConnection]= readConnections
  lazy val activities: List[Activity]=readActivities
  lazy val transformations: List[Transformation]= readTransformations

  def validateMetadata( jsonPath:String):(List[String],List[String], List[String])={
    val directoriesConnections = fs.directoriesWithSQLConnections(jsonPath)
      .map(_ + s"${sqlconnectionString}*.json")
    val directoriesWithDatasets = fs.directoriesWithTransformations(jsonPath)
      .map(_ + s"$transformationsString*.json")
    val directoriesWithActivities = fs.directoriesWithActivities(jsonPath)
      .map(_ + s"${activityString}*.json")
    (validateConnections(directoriesConnections),validateTransformations(directoriesWithDatasets), validateActivities(directoriesWithActivities))
  }

  private def readConnections: List[SQLConnection] = {

    val directoriesSources = fs.directoriesWithSQLConnections(metagoldProperties.metadata.basePathTransform)
      .map(_ + s"${sqlconnectionString}*.json")

    var ficheros: List[String] = List()

    for (w <- 0 to directoriesSources.length - 1) {
      //val lastOne = directoriesSources(w).split("/")(0)
      val lastOne = directoriesSources(w).replace(s"${sqlconnectionString}*.json","")
      val ficherosDirectorio: List[String] = getListOfFiles(lastOne, "sqlconnection.*\\.json$")
      ficheros = ficheros ++ ficherosDirectorio
    }

    import spark.implicits._

    val badDirectories = validateConnections(directoriesSources)

    val directoriostotal = directoriesSources diff badDirectories

    //Lectura inicial
    val connectionsWithCorrupts = spark.read
      .schema(Encoders.product[SQLConnection].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)

    // Lectura de json corruptos
    val rawFiles = spark.read
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)

    // Comprobación de existencia de errores
    if (rawFiles.columns.contains("_corrupt_record")) {
      val corruptJsonFiles =
        rawFiles
          .where("_corrupt_record is not null")
          .collect()

      // Log errores
      if (corruptJsonFiles.length > 0) {
        val msg = corruptJsonFiles.map(r => r.getString(0)).mkString("\n\n")
        logger.error("Corrupt connection json files detected:\n\n" + msg, None)
      }
    }

    // Devolvemos datos sin error
    connectionsWithCorrupts
      .where("name is not null")
      .as[SQLConnection].collectAsList.asScala.toList
  }

  private def readTransformations: List[Transformation] ={

    val directoriesWithDatasets = fs.directoriesWithTransformations(metagoldProperties.metadata.basePathTransform)
      .map(_ + s"$transformationsString*.json")

    var ficheros: List[String] = List()

    for (w <- 0 to directoriesWithDatasets.length - 1) {
//      val lastOne = directoriesWithDatasets(w).split("/")(0)
      val lastOne = directoriesWithDatasets(w).replace(s"$transformationsString*.json","")
      val ficherosDirectorio: List[String] = getListOfFiles(lastOne, "transform.*\\.json$")
      ficheros = ficheros ++ ficherosDirectorio
    }

    val badDirectories = validateTransformations(directoriesWithDatasets)

    val directoriostotal = directoriesWithDatasets diff badDirectories

    import spark.implicits._

    // Lectura inicial de ficheros
    val transformationsWithCorrupts = spark.read
      .schema(Encoders.product[Transformation].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)

    // Lectura de datasets corruptos
    val rawFiles = spark.read
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)

    // Comprobación de existencia de errores
    if (rawFiles.columns.contains("_corrupt_record")) {
      val corruptJsonFiles =
        rawFiles
          .where("_corrupt_record is not null")
          .collect()

      // Log errores
      if (corruptJsonFiles.length > 0) {
        val msg = corruptJsonFiles.map(r => r.getString(0)).mkString("\n\n")
        logger.error("Corrupt transformation json files detected:\n\n" + msg, None)
      }
    }

    // Devolución transformaciones correctas
    transformationsWithCorrupts
      .where("name is not null and typ is not null")
      .as[Transformation].collectAsList.asScala.toList


  }

  private def readActivities: List[Activity] ={

    val directoriesWithActivities = fs.directoriesWithActivities(metagoldProperties.metadata.basePathTransform)
      .map(_ + s"${activityString}*.json")

    var ficheros: List[String] = List()

    for (w <- 0 to directoriesWithActivities.length - 1) {
//      val lastOne = directoriesWithActivities(w).split("/")(0)
      val lastOne = directoriesWithActivities(w).replace(s"$activityString*.json","")
      val ficherosDirectorio: List[String] = getListOfFiles(lastOne, "activity.*\\.json$")
      ficheros = ficheros ++ ficherosDirectorio
    }

    import spark.implicits._

    //recogemos las rutas de json que son erróneas y las filtramos
    val badDirectories = validateActivities(directoriesWithActivities)

    val directoriostotal = ficheros diff badDirectories

    //Lectura inicial actividades
    val activitiesWithCorrupts = spark.read
      .schema(Encoders.product[Activity].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)

    // Lectura de json en raw
    val rawFiles = spark.read
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(directoriostotal: _*)

    // Comprobación de existencia de errores
    if (rawFiles.columns.contains("_corrupt_record")) {
      val corruptJsonFiles =
        rawFiles
          .where("_corrupt_record is not null")
          .collect()
      if (corruptJsonFiles.length > 0) {
        // Log errores
        val msg = corruptJsonFiles.map(r => r.getString(0)).mkString("\n\n")
        logger.error("Corrupt connection json files detected:\n\n" + msg, None)
      }
    }

    // Devolución de actividades no corruptas
    activitiesWithCorrupts
      .where("name is not null")
      .as[Activity].collectAsList.asScala.toList

  }

  def activityByName(name: String): Option[Activity] = {
    throw new NoSuchElementException("Esta versión del motor ya no soporta la búsqueda recursiva de actividades.\nLas actividades deben empezar por 'activity-' y contener el nombre de la carpeta source a continuación.\n{sourceFolder}/activity-{sourceFolder}-{activityName}/activity-{sourceFolder}-{activityName}.json")
    val matchedActivities = activities.filter(p = activity =>
      activity.name.equals(name)
    )

    if (matchedActivities.groupBy(_.name).keys.toList.length > 1) throw new NonUniqueDatasetException("Non unique transformation activity for name: " + name)

    matchedActivities.headOption

  }
  
  def activityByPath(path: String): Option[Activity] = {
    import spark.implicits._
    val temp = spark.read
      .schema(Encoders.product[Activity].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(this.metagoldProperties.metadata.basePathTransform+path)

    temp.as[Activity].collectAsList.asScala.toList.headOption
  }

  def transformationByName(name: String): Option[Transformation] = {
    throw new NoSuchElementException("Esta versión del motor ya no soporta la búsqueda recursiva de transformaciones.\nEl nombre de la transformación debe empezar por 'transform-' seguido del nombre que se define dentro de la actividad. Se deben guardar en la raiz de una carpeta con el mismo nombre que la carpeta de la actividad que las ejecuta (pero dentro de transformations).")
    logger.info(s"Looking for transformation $name")

    if (transformations == null){
      throw new NoSuchElementException("No metadata found for transformations")
    }

    val matchedTransformations = transformations.filter(p = transformation =>
      transformation.name.equals(name)
    )

    if (matchedTransformations.groupBy(_.name).keys.toList.length > 1) {
      //throw new NonUniqueDatasetException("Non unique transformation for name: " + name)
      logger.error("Non unique transformation for name: " + name,None)
      None
    }
    else if (matchedTransformations.groupBy(_.name).keys.toList.length == 0) {
      val msg = "Non transformation found for name: " + name + s"\nAvailable transformations found are: ${transformations.map(t=>t.name).mkString(",")}"
      //      throw new NoSuchElementException(msg)
      logger.error(msg,None)
      None
    }
    else
      matchedTransformations.headOption
  }

  def transformationByPath(path: String): Option[Transformation] = {
    import spark.implicits._
    val temp = spark.read
      .schema(Encoders.product[Transformation].schema)
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .json(this.metagoldProperties.metadata.basePathTransform+path)

    temp.as[Transformation].collectAsList.asScala.toList.headOption
  }

  def sqlConnectionByName(name: String): Option[SQLConnection] = {

    val matchedConnections = sqlConnections.filter(p = connection =>
      connection.name.equals(name)
    )

    if (matchedConnections.groupBy(_.name).keys.toList.length > 1) throw new NonUniqueDatasetException("Non unique connection for name: " + name)
    if (matchedConnections.groupBy(_.name).keys.toList.length < 1) throw new NonUniqueDatasetException("No connection found for name: " + name)

    matchedConnections.headOption
  }

  def validateConnections(directoriesConnections: List[String]): List[String] = {

    val jsonSchemaPath = this.getClass.getResource("/jsonSchemaFiles/schema-sqlConnection.json")
    val schemaJsonString = jsonSchemaPath.openStream()//jsonSchemaPath.getFile
    val schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaJsonString)

    var outputValues: List[String] = List()

    for (w <- 0 to directoriesConnections.length - 1) {
//      val lastOne = directoriesSources(w).split("/")(0)
      val lastOne = directoriesConnections(w).replace(s"$sqlconnectionString*.json","")

      val ficheros: List[String] = getListOfFiles(lastOne, "sqlconnection.*\\.json$")
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

  def validateTransformations(directoriesWithTransformations: List[String]): List[String] = {

    val jsonSchemaPath = this.getClass.getResource("/jsonSchemaFiles/schema-transformation.json")
    val schemaJsonString = jsonSchemaPath.openStream()//jsonSchemaPath.getFile
    val schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaJsonString)

    var outputValues: List[String] = List()

    for (w <- 0 to directoriesWithTransformations.length - 1) {
//      val lastOne = directoriesWithTransformations(w).split("/")(0)
      val lastOne = directoriesWithTransformations(w).replace(s"$transformationsString*.json","")
      val ficheros: List[String] = getListOfFiles(lastOne, "transform.*\\.json$")
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

  def validateActivities(directoriesWithActivities: List[String]): List[String] = {

    val jsonSchemaPath = this.getClass.getResource("/jsonSchemaFiles/schema-activity.json")
    val schemaJsonString = jsonSchemaPath.openStream()//jsonSchemaPath.getFile
    val schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(schemaJsonString)

    var outputValues: List[String] = List()

    for (w <- 0 to directoriesWithActivities.length - 1) {
//      val lastOne = directoriesWithActivities(w).split("/")(0)
      val lastOne = directoriesWithActivities(w).replace(s"$activityString*.json","")
      val ficheros: List[String] = getListOfFiles(lastOne, "activity.*\\.json$")
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

  def getListOfFiles(dir: String, pattern: String): List[String] = {
//    val file = new File(dir)
//    file.listFiles.filter(_.isFile)
//      .filter(_.getName.matches(pattern))
//      .map(_.getPath).toList
    val directory = dir.replace("\\","/")
    val files = fs.ls(directory)
    val jsonFiles = files.filter(_.replace("\\","/").replace(directory,"").matches(pattern))
    jsonFiles.toList
  }


}
