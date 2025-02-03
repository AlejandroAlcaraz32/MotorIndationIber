package com.minsait.indation

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.logging.Logging
import com.minsait.indation.activity.{IngestApiCallActivity, IngestBatchFileActivity, IngestBatchSourceActivity, IngestJdbcTableActivity, IngestStreamingTopicActivity}
import com.minsait.indation.execution.ExecutionParameters
import com.minsait.common.logging.GraylogLogger.graylogProperties
import com.minsait.common.spark.SparkSessionFactory
import com.minsait.common.utils.EncryptUtils
import com.minsait.common.utils.fs.SynapseFSCommands
import com.minsait.indation.metadata.exceptions.{DatasetException, SourceException}
import com.minsait.indation.metadata.models.enums.DatasetTypes.{Api, File, Table, Topic}
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.validators.DatasetValidator
import org.apache.commons.cli.{BasicParser, Options}

object Main extends Logging {

  def main(args: Array[String]): Unit = {

    var activitySuccess = false

    try {
      val options = new Options()

      logger.info("New Execution Received:")
      
      args.foreach{
        param=>logger.info(param)
      }
      
      ExecutionParameters.executionParameters.foreach(
        parameter => options.addOption(
          new org.apache.commons.cli.Option(parameter.shortName, parameter.longName, true, parameter.description)))

      val parse = new BasicParser()
      val runParametersLine = parse.parse(options, args)

      
      var propertiesFilePath = runParametersLine.getOptionValue(ExecutionParameters.ConfigFile.shortName)

      // Validamos si el fichero de configuración está en el datalake
      if (propertiesFilePath.startsWith("abfs")) {
        propertiesFilePath = SynapseFSCommands.importExternalFileIntoTemp(propertiesFilePath)
      }

      val configManager = new ConfigurationManager(propertiesFilePath)

      if (configManager.indationProperties.graylog.isDefined)
        graylogProperties = configManager.indationProperties.graylog //TODO: Ojo, esto es particular para un servidor graylog en modo http post para cubrir necesidades EA. No lo veo en Indation corporativo.
      else
        logger.info("No graylog server configured")
      
      logger.info("Configuring Spark Session...")
      SparkSessionFactory.configSparkSession(configManager.indationProperties)
      
      val triggerId = if (runParametersLine.hasOption(ExecutionParameters.AdfRunId.shortName)) {
        runParametersLine.getOptionValue(ExecutionParameters.AdfRunId.shortName)
      }
      else {
        ""
      }
      

      EncryptUtils.setProperties(configManager.indationProperties)

      // Validates Json
      if (runParametersLine.hasOption(ExecutionParameters.ValidateJson.shortName)) {
        val jsonPath = runParametersLine.getOptionValue(ExecutionParameters.ValidateJson.shortName)
        logger.info(s"Validate Json Command Received: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateJson.shortName)}")
        val metadataReader = new MetadataFilesManager(configManager.indationProperties)
        val jsonValidationResult=metadataReader.validateMetadata(jsonPath)
        val listbadsources = jsonValidationResult._1
        val listbaddatasets = jsonValidationResult._2
        if (!listbadsources.isEmpty & !listbaddatasets.isEmpty) {
          listbadsources.foreach(x => logger.error(s"Source malformed: " + x, None))
          listbaddatasets.foreach(x => logger.error(s"Datasets malformed: " + x, None))
          throw new SourceException(s"There are malformed sources and datasets: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateJson.shortName)}")
        }else if(!listbaddatasets.isEmpty & listbadsources.isEmpty) {
          listbaddatasets.foreach(x => logger.error(s"Datasets malformed: " + x, None))
          throw new SourceException(s"There are malformed sources: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateJson.shortName)}")
        }else if(listbaddatasets.isEmpty & !listbadsources.isEmpty){
          listbadsources.foreach(x => logger.error(s"Source malformed: " + x, None))
          throw new DatasetException(s"There are malformed datasets: ${runParametersLine.getOptionValue(ExecutionParameters.ValidateJson.shortName)}")
        }

      }

      // Process by file input
      if (runParametersLine.hasOption(ExecutionParameters.IngestFile.shortName)) {

        logger.info(s"Ingest File Execution Command Received: ${runParametersLine.getOptionValue(ExecutionParameters.IngestFile.shortName)}")
        //TODO: Incluir validación del dataset asociado al fichero DatasetValidator.validateDatasetColumns
        activitySuccess = new IngestBatchFileActivity(configManager.indationProperties)
          .executeWithFile(runParametersLine.getOptionValue(ExecutionParameters.IngestFile.shortName), triggerId)
      }

      // Process by dataset
      if (runParametersLine.hasOption(ExecutionParameters.IngestDataset.shortName) || runParametersLine.hasOption(ExecutionParameters.DatasetPath.shortName)) {

        val metadataReader = new MetadataFilesManager(configManager.indationProperties)
        val dataset_path = Option(runParametersLine.getOptionValue(ExecutionParameters.DatasetPath.shortName))
              .getOrElse(runParametersLine.getOptionValue(ExecutionParameters.IngestDataset.shortName))
        logger.info(s"Ingest Dataset Execution Command Received: ${dataset_path}")

        val dataset = if (dataset_path.endsWith(".json")) {
          // Directly use datasetByPath if the path ends with .json

          metadataReader.datasetByPath(dataset_path)
        } else {
          try {

            logger.info("Attempting to load dataset from constructed path: " + dataset_path)
            // Attempt to construct the path from dataset_path
            val extractedSourceName = dataset_path.split("-")(1) // Extracting the SourceName from dataset_path
            val constructedPath = s"$extractedSourceName/$dataset_path/$dataset_path.json"
            logger.info("Constructed Path: " + constructedPath)
            // Try to load the dataset using the constructed path
            metadataReader.datasetByPath(constructedPath)
          } catch {
            case _: Exception => 
              // If it fails, fall back to datasetByName
              logger.info("Could not find the dataset in constructed path.")
              throw new NoSuchElementException("Esta versión del motor ya no soporta la búsqueda recursiva de 'datasets'.\nLos datasets deben empezar por 'dataset-' y contener el nombre de la carpeta source a continuación.\nEl dataset se busca siguiendo la nomenclatura: {sourceFolder}/dataset-{sourceFolder}-{datasetName}/dataset-{sourceFolder}-{datasetName}.json")
              //metadataReader.datasetByName(dataset_path)
          }
        }

        val sourcePathName = if (runParametersLine.hasOption(ExecutionParameters.SourcePath.shortName)) {
          runParametersLine.getOptionValue(ExecutionParameters.SourcePath.shortName)
        } else {
          dataset_path.split("-")(1)
        }

        if(dataset.isDefined) {
          DatasetValidator.validateDatasetColumns(dataset.get, configManager.indationProperties)

          if (dataset.get.typ.equals(Topic)) {
            logger.info(s"Starting topictype dataset ${dataset.get.name} ingestion.")
            activitySuccess = new IngestStreamingTopicActivity(configManager.indationProperties)
              .execute(dataset.get, triggerId)
          }
          else if (dataset.get.typ.equals(Table)) {
            logger.info(s"Starting tabletype dataset ${dataset.get.name} ingestion.")
            activitySuccess = new IngestJdbcTableActivity(configManager.indationProperties)
              .execute(dataset.get, triggerId, sourcePathName)
          }
          else if (dataset.get.typ.equals(File)) {

            logger.info(s"Starting filetype dataset ${dataset.get.name} ingestion.")
            activitySuccess = new IngestBatchFileActivity(configManager.indationProperties)
              .executeWithDataset(dataset.get, triggerId, sourcePathName)

          }
          else if (dataset.get.typ.equals(Api)) {
            logger.info(s"Starting apitype dataset ${dataset.get.name} ingestion.")
            activitySuccess = new IngestApiCallActivity(configManager.indationProperties)
              .execute(dataset.get, triggerId)
          }
        }
        else if (!dataset.isDefined) {
          logger.error(s"Dataset not defined: ${runParametersLine.getOptionValue(ExecutionParameters.IngestDataset.shortName)}", None)
          throw new DatasetException(s"Dataset not defined: ${runParametersLine.getOptionValue(ExecutionParameters.IngestDataset.shortName)}")
        }
      }

      // Process source by name IngestSource
      if (runParametersLine.hasOption(ExecutionParameters.IngestSource.shortName)) {

        val sourceName = runParametersLine.getOptionValue(ExecutionParameters.IngestSource.shortName)
        val metadataManager = new MetadataFilesManager(configManager.indationProperties)
        val source = metadataManager.sourceByName(sourceName)

        if (source.isEmpty) {
          logger.error("Source " + sourceName + " not found.", None)
          throw new SourceException("Source " + sourceName + " not found.")
        }

        logger.info("Starting source " + sourceName + " ingestion.")
        activitySuccess = IngestBatchSourceActivity.init(source.get, configManager, triggerId)
      }
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Unexpected error in main execution", Some(ex))
        //TODO: Homogeneizar tratamiento de excepciones. O todas las que desembocan en que una ingesta no puede iniciarse propagan el error
        // o ninguna propaga el error y en Main se capturan todas las posibles excepciones
        // de momento, lo vamos a propagar para ver el error en Data Factory
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
