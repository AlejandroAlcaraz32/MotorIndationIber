package com.minsait.metagold

import com.minsait.metagold.activity.ActivityProcess
import com.minsait.metagold.activity.statistics.ActivityStatisticsTableWriter
import com.minsait.metagold.activity.statistics.models.{ActivityResults, ActivityStatistics}
import com.minsait.common.configuration.ConfigurationManager
import com.minsait.metagold.execution.ExecutionParameters
import com.minsait.metagold.logging.GraylogLogger.graylogProperties
import com.minsait.metagold.logging.Logging
import com.minsait.metagold.metadata.MetadataFilesManager
import com.minsait.common.spark.SparkSessionFactory
import com.minsait.metagold.metadata.exceptions.MetadataValueException
import org.apache.commons.cli.{BasicParser, Options}


object Main extends Logging {

  var statistics: ActivityStatistics = _
  var statsWriter: ActivityStatisticsTableWriter = _

  def main(args: Array[String]): Unit = {

    val uuid = java.util.UUID.randomUUID.toString()
    var publishStatistics = true
    var activitySuccess = false
    try {

      val options = new Options()

      logger.info(s"New Gold Execution Received:")

      ExecutionParameters.executionParameters.foreach(
        parameter => options.addOption(
          new org.apache.commons.cli.Option(parameter.shortName, parameter.longName, true, parameter.description)))
      

      val parse = new BasicParser()
      val runParametersLine = parse.parse(options, args)

      var propertiesFilePath = runParametersLine.getOptionValue(ExecutionParameters.ConfigFile.shortName)

      val configManager = new ConfigurationManager(propertiesFilePath)

      // Configuramos logger de graylog si se ha definido
      if (configManager.indationProperties.graylog.isDefined)
        graylogProperties = configManager.indationProperties.graylog //TODO: Ojo, esto es particular para un servidor graylog en modo http post para cubrir necesidades EA. No lo veo en Indation corporativo.
      else
        logger.info("No graylog server configured")
      
      logger.info("Configuring Spark Session...")
      SparkSessionFactory.configSparkSession(configManager.indationProperties)

      //logger.info("Añadiendo parámetros de ejecución...")
      // Añadimos todos los posibles parámetros de ejecución de todas las actividades
      /*
      val metadataManager = new MetadataFilesManager(configManager.indationProperties)
      metadataManager.activities.foreach(activity => {
        if (activity.parameters.isDefined)
          activity.parameters.get.foreach(parameter => {
            options.addOption(
              new org.apache.commons.cli.Option(parameter.shortName, parameter.longName, true, parameter.description)
            )
          })
      })
      */

      // Capturamos identificador de pipeline de Data Factory de los parámetros
      val triggerId = if (runParametersLine.hasOption(ExecutionParameters.AdfRunId.shortName)) {
        runParametersLine.getOptionValue(ExecutionParameters.AdfRunId.shortName)
      }
      else {
        ""
      }

      // Desactivamos publicación de estadísticas si solicitado por parámetro
      if (runParametersLine.hasOption(ExecutionParameters.PublishStatistics.shortName) && runParametersLine.getOptionValue(ExecutionParameters.PublishStatistics.shortName) == "false") {
        publishStatistics = false
      }

      //Conexiones
      if (runParametersLine.hasOption(ExecutionParameters.ValidateSQLConnections.shortName)) {
        val jsonPath = runParametersLine.getOptionValue(ExecutionParameters.ValidateSQLConnections.shortName)
        logger.info(s"Validate SQLConnections Command Received: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateSQLConnections.shortName)}")
        val metadataReader = new MetadataFilesManager(configManager.indationProperties)
        val jsonValidationResult = metadataReader.validateMetadata(jsonPath)
        val listbadConnections = jsonValidationResult._1
        if (!listbadConnections.isEmpty) {
          listbadConnections.foreach(x => logger.error(s"SQLConnection JSON malformed: " + x, None))
          throw new MetadataValueException(s"There are malformed SQLConnection JSON: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateSQLConnections.shortName)}")
        }
      }

      //Transformations
      if (runParametersLine.hasOption(ExecutionParameters.ValidateTransformations.shortName)) {
        val jsonPath = runParametersLine.getOptionValue(ExecutionParameters.ValidateTransformations.shortName)
        logger.info(s"Validate Transformation Command Received: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateTransformations.shortName)}")
        val metadataReader = new MetadataFilesManager(configManager.indationProperties)
        val jsonValidationResult = metadataReader.validateMetadata(jsonPath)
        val listbadTransformations = jsonValidationResult._2
        if (!listbadTransformations.isEmpty) {
          listbadTransformations.foreach(x => logger.error(s"Transformations JSON malformed: " + x, None))
          throw new MetadataValueException(s"There are malformed Transformations JSON: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateTransformations.shortName)}")
        }
      }

      //Activities
      if (runParametersLine.hasOption(ExecutionParameters.ValidateActivities.shortName)) {
        val jsonPath = runParametersLine.getOptionValue(ExecutionParameters.ValidateActivities.shortName)
        logger.info(s"Validate Transformation Command Received: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateActivities.shortName)}")
        val metadataReader = new MetadataFilesManager(configManager.indationProperties)
        val jsonValidationResult = metadataReader.validateMetadata(jsonPath)
        val listbadActivities = jsonValidationResult._3
        if (!listbadActivities.isEmpty) {
          listbadActivities.foreach(x => logger.error(s"Activities JSON malformed: " + x, None))
          throw new MetadataValueException(s"There are malformed Activities JSON: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateActivities.shortName)}")
        }
      }
      
      logger.info("Iniciando actividad!")
      // Procesamos actividad
      val transformationActivityId: String = if (runParametersLine.hasOption(ExecutionParameters.TransformActivityByPath.shortName)) {
        runParametersLine.getOptionValue(ExecutionParameters.TransformActivityByPath.shortName)
      } else {
        runParametersLine.getOptionValue(ExecutionParameters.TransformActivity.shortName)
      }

      logger.info(s"Activity Id: $transformationActivityId")

      val transformationActivityProcess = new ActivityProcess(configManager.indationProperties, runParametersLine)
      
      logger.info("Initializing statsWriter...")
      statsWriter = new ActivityStatisticsTableWriter(configManager.indationProperties)
      statistics = transformationActivityProcess.execute(transformationActivityId, triggerId, args)

      // Se registran las estadísticas
      logger.info("Statistics object created")
      if (publishStatistics)
        statsWriter.receiveStatistics(statistics.copy(uuid = uuid))

      // Registramos resultado
      if (statistics.activityResult == ActivityResults.Success) {
        activitySuccess = true
      }
    }

    catch {
      case ex: Throwable =>
        logger.error("Unexpected error raised in main execution.", Some(ex))
        if (statistics != null && statsWriter != null && publishStatistics) {
          statsWriter.receiveStatistics(statistics.copy(uuid = uuid))
//          try {
//            statsWriter.receiveStatistics(statistics.copy(uuid = uuid))
//          }
//          catch {
//            case ex2: Throwable=>
//              logger.error("Error receiving statistics.", Some(ex2))
//          }
        }
    }
    finally{
      try{
        logger.info("Clearing cache...")
        SparkSessionFactory.temporaryViews.foreach(viewName =>{
          logger.info(s"Dropping temporary view ${viewName}")
          SparkSessionFactory.spark.catalog.dropTempView(viewName)
        })
        SparkSessionFactory.temporaryViews = List()

        SparkSessionFactory.persistedDataframes.foreach(df =>{
          df.unpersist()
        })
        SparkSessionFactory.persistedDataframes = List()

        // SparkSessionFactory.spark.sqlContext.clearCache()
        logger.info("Stopping Spark session...")
        SparkSessionFactory.stopSparkSession()
        logger.info("Process ended successfully!")
      }
      catch {
        case ex:Throwable =>{
          logger.error("Error clearing cache.",Some(ex))
        }
      }

      // Si la actividad no ha tenido éxito, provocamos una excepción para informar al orquestador
      // TODO: Deberíamos evaluar si nos interesa o no este comportamiento
      if(!activitySuccess){
        throw  new RuntimeException(s"Error executing transformation activity: ${args.mkString(" ")}")
      }
    }
  }
}
