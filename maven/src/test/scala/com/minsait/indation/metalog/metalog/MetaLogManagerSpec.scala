package com.minsait.indation.metalog.metalog

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.indation.metalog.MetaInfo.{Layers, Statuses}
import com.minsait.indation.metalog.models.{DatasetLog, Ingestion, SourceLog}
import com.minsait.indation.metalog.{MetaInfo, MetaLogManager}
import org.h2.tools.RunScript
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.InputStreamReader
import java.sql.{Connection, DriverManager}

class MetaLogManagerSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll {

  var dbConn: Connection = _

  override def beforeAll() {
    Class.forName("org.h2.Driver")
    dbConn = DriverManager.getConnection("jdbc:h2:mem:indation_tests;MODE=PostgreSQL", "sa", "")

    val statement = dbConn.prepareStatement("DROP ALL OBJECTS")
    statement.execute()

    RunScript.execute(dbConn, new InputStreamReader(this.getClass.getResourceAsStream("/SQL/Metalog/TestData/CreateMetadataSchema.sql")))
    RunScript.execute(dbConn, new InputStreamReader(this.getClass.getResourceAsStream("/SQL/Metalog/TestData/CreateMetalogSchema.sql")))
    RunScript.execute(dbConn, new InputStreamReader(this.getClass.getResourceAsStream("/SQL/Metadata/TestData/CreateTestSchema.sql")))
    RunScript.execute(dbConn, new InputStreamReader(this.getClass.getResourceAsStream("/SQL/Metadata/TestData/TestData.sql")))
    RunScript.execute(dbConn, new InputStreamReader(this.getClass.getResourceAsStream("/SQL/Metalog/TestData/CreateTestSchema.sql")))
    RunScript.execute(dbConn, new InputStreamReader(this.getClass.getResourceAsStream("/SQL/Metalog/TestData/MetalogTestData.sql")))
  }

  test("MetaLogManager#initIngestionLog Create a instance of Ingestion Log") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // When
    val ingestionLog = metalogManager.initIngestionLog(Layers.BronzeLayerName)

    // Then
    assert(ingestionLog.isInstanceOf[Ingestion])
    assert(ingestionLog.status == Statuses.RunningState)
    assert(ingestionLog.layer.get == MetaInfo.Layers.BronzeLayerName)
  }

  test("MetaLogManager#initIngestionLog Create a instance of Source Log") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // When
    val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    val sourceLog = metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

    // Then
    assert(sourceLog.isInstanceOf[SourceLog])
    assert(sourceLog.status == MetaInfo.Statuses.RunningState)
    assert(sourceLog.ingestionId == ingestionLog.ingestionId)
  }

  test("MetaLogManager#initIngestionLog Create a instance of Dataset Log") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // When
    val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    val sourceLog = metalogManager.initSourceLog(ingestionLog.ingestionId, 1)
    val datasetLog = metalogManager.initDatasetLog(sourceLog.sourceLogId, 1)

    // Then
    assert(datasetLog.isInstanceOf[DatasetLog])
    assert(datasetLog.status == MetaInfo.Statuses.RunningState)
    assert(sourceLog.ingestionId == ingestionLog.ingestionId)
    assert(datasetLog.sourceLogId == sourceLog.sourceLogId)
  }

  test("MetaLogManager#initIngestionLog finish Ingestion Log") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // When
    val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    Thread.sleep(100)
    val endIngestionLog = metalogManager.finishIngestionLog(ingestionLog)

    // Then
    assert(endIngestionLog.isInstanceOf[Ingestion])
    assert(endIngestionLog.status == MetaInfo.Statuses.FinishState)
    assert(endIngestionLog.duration.get > 0)
  }

  test("MetaLogManager#initIngestionLog finish Source Log") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // When
    val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    val sourceLog = metalogManager.initSourceLog(ingestionLog.ingestionId, 1)
    Thread.sleep(100)
    val sourceLogFinished = metalogManager.finishSourceLog(sourceLog)
    sourceLogFinished.result = MetaInfo.Result.WithErrors

    // Then
    assert(sourceLogFinished.isInstanceOf[SourceLog])
    assert(sourceLogFinished.status == MetaInfo.Statuses.FinishState)
    assert(sourceLogFinished.duration.get > 0)
    assert(sourceLogFinished.result == MetaInfo.Result.WithErrors)
  }

  test("MetaLogManager#initIngestionLog finish Dataset Log") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)


    // When
    val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    val sourceLog = metalogManager.initSourceLog(ingestionLog.ingestionId, 1)
    val datasetLog = metalogManager.initDatasetLog(sourceLog.sourceLogId, 1)
    Thread.sleep(100)
    val finishedDatasetLog = metalogManager.finishDatasetLog(datasetLog)


    // Then
    assert(finishedDatasetLog.isInstanceOf[DatasetLog])
    assert(finishedDatasetLog.status == MetaInfo.Statuses.FinishState)
    assert(finishedDatasetLog.duration.get > 0)
  }

  test("MetaLogManager#getPendingSilver existing source id should return a non-empty List[DatasetLog]") {
    // Given
    val sourceId = 1
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // When
    val datasetLogList = metalogManager.getPendingSilver(sourceId)

    // Then
    assert(datasetLogList.size == 2)
    assert(datasetLogList.head.pendingSilver.getOrElse(false))
    assert(datasetLogList.head.executionEnd.isEmpty)
    assert(datasetLogList.last.pendingSilver.getOrElse(false))
    assert(datasetLogList.last.executionEnd.isDefined)
  }

	test("MetaLogManager#existsConcurrentExecution Not exists running execution in 6h window BRONZE") {

		// Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

		val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
		metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

		val statement = dbConn.prepareStatement("UPDATE metalog.source_log set executionStart = '2020-06-04 00:08:39'")
		statement.execute()
		// Then
		assert(!metalogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, 1))
	}


	test("MetaLogManager#existsConcurrentExecution Not exists running execution window BRONZE") {

		// Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

		val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
		metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

		val statement = dbConn.prepareStatement("UPDATE metalog.source_log set status = '" + MetaInfo.Statuses.FinishState + "'")
		statement.execute()

		// Then
		assert(!metalogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, 1))
	}

	test("MetaLogManager#existsConcurrentExecution Exists running execution in 6h window BRONZE") {

		// Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

		val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
		metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

		// Then
		assert(metalogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, 1))
	}

	test("MetaLogManager#existsConcurrentExecution Not exists running execution in 6h window SILVER") {

		// Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

		val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)
		metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

		val statement = dbConn.prepareStatement("UPDATE metalog.source_log set executionStart = '2020-06-04 00:08:39'")
		statement.execute()
		// Then
		assert(!metalogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, 1))
	}


	test("MetaLogManager#existsConcurrentExecution Not exists running execution window SILVER") {

		// Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

		val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)
		metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

		val statement = dbConn.prepareStatement("UPDATE metalog.source_log set status = '" + MetaInfo.Statuses.FinishState + "'")
		statement.execute()

		// Then
		assert(!metalogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, 1))
	}

	test("MetaLogManager#existsConcurrentExecution Exists running execution in 6h window SILVER") {

		// Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

		val ingestionLog = metalogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)
		metalogManager.initSourceLog(ingestionLog.ingestionId, 1)

		// Then
		assert(metalogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, 1))
	}

  test("MetaLogManager#isFullReprocess false") {

    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // Then
    assert(!metalogManager.isFullReprocess("FileNotProcessed.csv"))
  }

  test("MetaLogManager#isFullReprocess true FINISHED_WITH_ERRORS") {
    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // Then
    assert(metalogManager.isFullReprocess("FILE_20180314.csv"))
  }

  test("MetaLogManager#isFullReprocess true FINISHED_OK") {
    // Given
    val propertiesFilePath = this.getClass.getResource("/indation-h2-config.yml").getPath
    val configManager = new ConfigurationManager(propertiesFilePath)
    val metalogManager = new MetaLogManager(configManager)

    // Then
    assert(metalogManager.isFullReprocess("FILE_20180315.csv"))
  }
}
