package com.minsait.indation.activity

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.bronze.BronzeEngine
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.models.Source
import com.minsait.indation.metadata.models.enums.SourceTypes
import com.minsait.indation.metalog.MetaLogManager
import com.minsait.indation.silver.SilverEngine

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object IngestBatchSourceActivity extends Logging with SparkSessionWrapper {

  def init(source: Source, configManager: ConfigurationManager, triggerId: String): Boolean = {

    if (source.typ == SourceTypes.Jdbc){
      initJdbc(source, configManager.indationProperties, triggerId)
    }
    else {
      val datalakeManager = new DatalakeManager(configManager)
      val metalogManager = new MetaLogManager(configManager)

      logger.info("Starting Bronze process.")
      val bronzeEngine = new BronzeEngine(datalakeManager, metalogManager)
      bronzeEngine.process(source)

      logger.info("Starting Silver process.")
      val silverEngine = new SilverEngine(datalakeManager, metalogManager)
      silverEngine.process(source)

      // Si no hay excepciones, lo damos por bueno
      true

    }

//    if (configManager.getEnvironment == EnvironmentTypes.Local) {
//      spark.close()
//    }

  }

  def initJdbc(source: Source, indationProperties: IndationProperties, triggerId: String): Boolean = {

    var success = true

    // TODO: Ampliar a los diferentes tipos de Metadata Manager.
    val metadataManager = new MetadataFilesManager(indationProperties)

    // Get datasets from source
    val datasets = metadataManager.datasetBySource(source)

    // Process all datasets in parallel using futures
    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    var futures = List[Future[Boolean]] ()
    datasets.foreach(dataset =>{
      logger.info(s"Starting dataset ${dataset.name} ingestion.")
      val future = Future(ingestJdbcTableActivity.execute(dataset, triggerId, "None"))
      futures = futures :+ future
    })
    futures.foreach(f=> {
      val result = Await.result(f, Duration.Inf) // Espera indefinidamente hasta que la ejecución termina}
      if (!result){
        success=false
      }
    }
    )

    // Retornamos resultado
    success

  }
}
