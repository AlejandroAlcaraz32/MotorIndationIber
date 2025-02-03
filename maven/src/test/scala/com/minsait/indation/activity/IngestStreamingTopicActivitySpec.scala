package com.minsait.indation.activity

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.{BuildInfo, FileNameUtils}
import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
import com.minsait.indation.datalake.readers.LandingTopicReader
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Dataset, Source}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StringType}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.enrichString

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

class IngestStreamingTopicActivitySpec extends AnyFunSuite with MockitoSugar  with BeforeAndAfterAll with BeforeAndAfterEach
  with DatasetComparer with SparkSessionWrapper {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\",DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val dataLakeStorageDate = "datalake_load_date"
  private val dataLakeIngestionUuid = "datalake_ingestion_uuid"
  private val yyyy = "yyyy"
  private val mm = "mm"
  private val dd = "dd"
  private val adfRunId = "adf-uuid"

  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("",testResourcesBasePath,"","")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("",testResourcesBasePath,"")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(Paths.get(testResourcesBasePath + "silver").toFile)
  }
  override def beforeEach(): Unit = {
    super.beforeEach()
    // clean full metastore
    spark.catalog.listDatabases.collect().foreach(db => {
      if (db.name != "default") {
        spark.sql("DROP DATABASE IF EXISTS " + db.name + " CASCADE")
      }
    })
    // clean full silver
    FileUtils.deleteDirectory(Paths.get(testResourcesBasePath + "silver").toFile)

  }
  private def ingestionStats(): List[ActivityStatistics] = {

    spark.sql("select * from applications.indation order by " + dataLakeStorageDate + " desc")
      .toJSON
      .collect
      .toList
      .map(_.parseJson.convertTo[ActivityStatistics])
  }

  private def assertInfoColumns(df: DataFrame): Unit = {
    assert(df.columns.contains(dataLakeStorageDate))
    assert(df.columns.contains(dataLakeIngestionUuid))
  }

  private def assertActivityStatistics(activityStatistics: ActivityStatistics,
                                       filePath: String,
                                       result: ActivityResults.ActivityResult,
                                       unknownAbsolutePath: Option[String],
                                       corruptedAbsolutePath: Option[String],
                                       schemaMismatchAbsolutePath: Option[String],
                                       invalidAbsolutePath: Option[String],
                                       bronzeAbsolutePath: Option[String],
                                       silverAbsolutePath: Option[String],
                                       bronzeValidRows: Option[Long],
                                       bronzeInvalidRows: Option[Long],
                                       silverValidRows: Option[Long],
                                       silverInvalidRows: Option[Long],
                                       silverPersistente: Option[ActivitySilverPersistence]
                                      ): Unit = {
    assertResult(ActivityTriggerTypes.Adf)(activityStatistics.trigger.typ)
    assertResult(adfRunId)(activityStatistics.trigger.id)
    assertResult(BuildInfo.name)(activityStatistics.engine.get.name)
    assertResult(BuildInfo.version)(activityStatistics.engine.get.version)
    assertResult(filePath)(activityStatistics.origin)
    assertResult(result)(activityStatistics.result)
    assertResult(true)(activityStatistics.execution.start.compareTo(new Timestamp(System.currentTimeMillis)) < 0)
    assertResult(true)(activityStatistics.execution.end.compareTo(new Timestamp(System.currentTimeMillis)) < 0)
    assertResult(true)(activityStatistics.execution.duration > 0)
    assertResult(unknownAbsolutePath.getOrElse(None))(activityStatistics.output_paths.unknown.getOrElse(None))
    assertResult(corruptedAbsolutePath.getOrElse(None))(activityStatistics.output_paths.corrupted.getOrElse(None))
    assertResult(schemaMismatchAbsolutePath.getOrElse(None))(activityStatistics.output_paths.schema_mismatch.getOrElse(None))
    assertResult(invalidAbsolutePath.getOrElse(None))(activityStatistics.output_paths.invalid.getOrElse(None))
    assertResult(bronzeAbsolutePath.getOrElse(None))(activityStatistics.output_paths.bronze.getOrElse(None))
    assertResult(silverAbsolutePath.getOrElse(None))(activityStatistics.output_paths.silver_principal.getOrElse(None))
    if(activityStatistics.rows.isDefined) {
      assertResult(bronzeValidRows.getOrElse(None))(activityStatistics.rows.get.bronze_valid.getOrElse(None))
      assertResult(bronzeInvalidRows.getOrElse(None))(activityStatistics.rows.get.bronze_invalid.getOrElse(None))
      assertResult(silverInvalidRows.getOrElse(None))(activityStatistics.rows.get.silver_invalid.getOrElse(None))
      assertResult(silverValidRows.getOrElse(None))(activityStatistics.rows.get.silver_valid.getOrElse(None))
    } else {
      assertResult(None)(activityStatistics.rows)
    }
    if(activityStatistics.silver_persistence.isDefined) {
      assertResult(silverPersistente.get.database.getOrElse(None))(activityStatistics.silver_persistence.get.database.getOrElse(None))
      assertResult(silverPersistente.get.principal_table.getOrElse(None))(activityStatistics.silver_persistence.get.principal_table.getOrElse(None))
      assertResult(silverPersistente.get.principal_previous_version.getOrElse(None))(activityStatistics.silver_persistence.get.principal_previous_version.getOrElse(None))
      assertResult(silverPersistente.get.principal_current_version.getOrElse(None))(activityStatistics.silver_persistence.get.principal_current_version.getOrElse(None))
      assertResult(silverPersistente.get.historical_table.getOrElse(None))(activityStatistics.silver_persistence.get.historical_table.getOrElse(None))
      assertResult(silverPersistente.get.historical_previous_version.getOrElse(None))(activityStatistics.silver_persistence.get.historical_previous_version.getOrElse(None))
      assertResult(silverPersistente.get.historical_current_version.getOrElse(None))(activityStatistics.silver_persistence.get.historical_current_version.getOrElse(None))
    } else {
      assertResult(None)(activityStatistics.silver_persistence)
    }
  }

  private def assertActivityDataset(activityStatistics: ActivityStatistics,
                                    name: String,
                                    ingestion: IngestionTypes.IngestionType,
                                    validation: ValidationTypes.ValidationType
                                   ): Unit = {
    assertResult(name)(activityStatistics.dataset.get.name)
    assertResult(DatasetTypes.Topic)(activityStatistics.dataset.get.typ)
    assertResult(1)(activityStatistics.dataset.get.version)
    assertResult(ingestion)(activityStatistics.dataset.get.ingestion_mode)
    assertResult(validation)(activityStatistics.dataset.get.validation_mode)
    assertResult("")(activityStatistics.dataset.get.partition_by)
  }
/*
  test("IngestStreamingTopicActivitySpec#event hub topic") {
    val landindTopicReader: LandingTopicReader = new LandingTopicReader(indationProperties) //mock[LandingTopicReader]
    val ingestStreamingTopicActivity = new IngestStreamingTopicActivity(indationProperties, landindTopicReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("topic-maier")
    ingestStreamingTopicActivity.execute(dataset.get, adfRunId)
  }
*/
  test("IngestStreamingTopicActivitySpec#empty topic") {
    val landindTopicReader: LandingTopicReader = mock[LandingTopicReader]
    val ingestStreamingTopicActivity = new IngestStreamingTopicActivity(indationProperties, landindTopicReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("atributos")

    when(landindTopicReader.readTopic(any[Source], any[Dataset])).thenReturn((spark.emptyDataFrame, "avrofile"))

    ingestStreamingTopicActivity.execute(dataset.get, adfRunId)
  }

  test("IngestStreamingTopicActivitySpec#schema mismatch topic") {
    val landindTopicReader: LandingTopicReader = mock[LandingTopicReader]
    val ingestStreamingTopicActivity = new IngestStreamingTopicActivity(indationProperties, landindTopicReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("schema-mismatch-topic")

    val schema = List(
      ("name", StringType, true),
      ("country", StringType, true),
      ("subcountry", StringType, true),
      ("geonameid", LongType, true)
    )

    val dataframe = spark.createDF(
      List(
        ("les Escaldes", "Andorra", "Escaldes-Engordany", 3040051L),
        ("Andorra la Vella", "Andorra", "Andorra la Vella", 3041563L)
      ),
      schema
    )

    val avroFilePath = this.testResourcesBasePath + "landing/streaming/tmp/20200102_worldCities.bz2.avro"
    when(landindTopicReader.readTopic(any[Source], any[Dataset])).thenReturn((dataframe, avroFilePath))

    ingestStreamingTopicActivity.execute(dataset.get, adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "schema-mismatch/source-kafka/schema-mismatch-topic/")

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
      dirs + "20200102_worldCities.bz2.avro"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath, ActivityResults.RejectedSchemaMismatch,None,
      None, Some(expectedSchemaMismatchPath), None, None,
      None, Some(0), Some(2), None, None, None)

    assertActivityDataset(activityStatistics, "schema-mismatch-topic", IngestionTypes.Incremental, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)) )
    assert(Files.notExists(Paths.get(avroFilePath)))
  }

  test("IngestStreamingTopicActivitySpec#topic ingestion ok") {
    val landindTopicReader: LandingTopicReader = mock[LandingTopicReader]
    val ingestStreamingTopicActivity = new IngestStreamingTopicActivity(indationProperties, landindTopicReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("topic-worldcities")

    val avroFilePath = this.testResourcesBasePath + "landing/streaming/tmp/20200101_worldCities.bz2.avro"
    val dataframe = this.spark.read.format("avro").load(avroFilePath)

    when(landindTopicReader.readTopic(any[Source], any[Dataset])).thenReturn((dataframe, avroFilePath))

    ingestStreamingTopicActivity.execute(dataset.get, adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-kafka/topic-worldcities/")

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      dirs + "20200101_worldCities.bz2.avro"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/interno/streaming/worldcities"

    val bronzeDF = spark.read.format("avro").load(expectedBronzePath)
    val silverDF = spark.sql("select * from streaming.worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath, ActivityResults.Ingested_Without_Errors,None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("streaming"),Some("worldcities"),None,Some(0L),
        None,None,None)))

    assertActivityDataset(activityStatistics, "topic-worldcities", IngestionTypes.Incremental, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(avroFilePath)))
  }
}
