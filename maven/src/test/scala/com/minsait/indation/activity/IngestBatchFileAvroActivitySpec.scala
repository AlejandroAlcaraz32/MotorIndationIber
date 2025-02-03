package com.minsait.indation.activity

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.BuildInfo
import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, ValidationTypes}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.enrichString

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

class IngestBatchFileAvroActivitySpec extends AnyFunSuite with MockitoSugar  with BeforeAndAfterAll with BeforeAndAfterEach
  with DatasetComparer with SparkSessionWrapper {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\",DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

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

  private val dataLakeStorageDate = "datalake_load_date"
  private val dataLakeIngestionUuid = "datalake_ingestion_uuid"
  private val yyyy = "yyyy"
  private val mm = "mm"
  private val dd = "dd"
  private val historicalSuffix = "_historical"
  private val adfRunId = "adf-uuid"

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
    assertResult(this.indationProperties.landing.basePath + filePath)(activityStatistics.origin)
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
    assertResult(DatasetTypes.File)(activityStatistics.dataset.get.typ)
    assertResult(1)(activityStatistics.dataset.get.version)
    assertResult(ingestion)(activityStatistics.dataset.get.ingestion_mode)
    assertResult(validation)(activityStatistics.dataset.get.validation_mode)
    assertResult("yyyy/mm/dd")(activityStatistics.dataset.get.partition_by)
  }

  private def assertInfoColumns(df: DataFrame): Unit = {
    assert(df.columns.contains(dataLakeStorageDate))
    assert(df.columns.contains(dataLakeIngestionUuid))
    assert(df.columns.contains(yyyy))
    assert(df.columns.contains(mm))
    assert(df.columns.contains(dd))
  }

  test("IngestBatchFileActivity#avro ingestion full-snapshot fail-fast ok") {
    val filePath = "pending/source2/20200101_worldCities.bz2.avro"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source2/full-snapshot-worldcities-avro/2020/01/01/20200101_worldCities.bz2.avro"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database2/full_snapshot_worldcities_avro"

    val bronzeDF = spark.read.format("avro").load(expectedBronzePath)
    val silverDF = spark.sql("select * from database2.full_snapshot_worldcities_avro")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database2"),Some("full_snapshot_worldcities_avro"),None,Some(0L),
        Some("full_snapshot_worldcities_avro_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-avro", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileActivity#avro ingestion full-snapshot fail-fast schema-mismatch") {
    val filePath = "pending/source2/20200101_worldCities_subset.bz2.avro"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatch = this.indationProperties.landing.basePath  +
      "schema-mismatch/source2/full-snapshot-worldcities-subset-avro/2020/01/01/20200101_worldCities_subset.bz2.avro"

    val notExpectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source2/full-snapshot-worldcities-subset-avro/2020/01/01/20200101_worldCities_subset.bz2.avro"

    val notExpectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database2/full_snapshot_worldcities_subset_avro"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch,None,
      None, Some(expectedSchemaMismatch), None, None,
      None, Some(0), Some(23018), None, None,
      None)

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-subset-avro", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedSchemaMismatch)))
    assert(Files.notExists(Paths.get(notExpectedBronzePath)))
    assert(Files.notExists(Paths.get(notExpectedSilverPath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }
}
