package com.minsait.indation.activity

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
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
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.enrichString

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

class IngestBatchFileJsonActivitySpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with BeforeAndAfterEach
  with DatasetComparer with SparkSessionWrapper {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\", DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("", testResourcesBasePath, "", "")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("", testResourcesBasePath, "")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  private val dataLakeStorageDate = "datalake_load_date"
  private val dataLakeStorageDay = "datalake_load_day"
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
    if (activityStatistics.rows.isDefined) {
      assertResult(bronzeValidRows.getOrElse(None))(activityStatistics.rows.get.bronze_valid.getOrElse(None))
      assertResult(bronzeInvalidRows.getOrElse(None))(activityStatistics.rows.get.bronze_invalid.getOrElse(None))
      assertResult(silverInvalidRows.getOrElse(None))(activityStatistics.rows.get.silver_invalid.getOrElse(None))
      assertResult(silverValidRows.getOrElse(None))(activityStatistics.rows.get.silver_valid.getOrElse(None))
    } else {
      assertResult(None)(activityStatistics.rows)
    }
    if (activityStatistics.silver_persistence.isDefined) {
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

  test("IngestBatchFileActivity#json ingestion full-snapshot fail-fast ok") {
    val filePath = "pending/source-json-5/20200101_worldCities.json"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/full-snapshot-worldcities-json/2020/01/01/20200101_worldCities.json.gz"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database5/full_snapshot_worldcities_json"

    val bronzeDF = spark.read.format("json").load(expectedBronzePath)
    val silverDF = spark.sql("select * from database5.full_snapshot_worldcities_json")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database5"), Some("full_snapshot_worldcities_json"), None, Some(0L),
        Some("full_snapshot_worldcities_json_historical"), None, Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-json", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileActivity#json ingestion full-snapshot fail-fast bz2 ok") {
    val filePath = "pending/source-json-5/20200101_worldCities.json.bz2"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/full-snapshot-worldcities-json-bz2/2020/01/01/20200101_worldCities.json.bz2"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database5/full_snapshot_worldcities_json_bz2"

    val bronzeDF = spark.read.format("json").load(expectedBronzePath)
    val silverDF = spark.sql("select * from database5.full_snapshot_worldcities_json_bz2")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database5"), Some("full_snapshot_worldcities_json_bz2"), None, Some(0L),
        Some("full_snapshot_worldcities_json_bz2_historical"), None, Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-json-bz2", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileActivity#json ingestion full-snapshot fail-fast ko schema mismatch") {
    val filePath = "pending/source-json-5/20200102_worldCities.json"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatch = this.indationProperties.landing.basePath +
      "schema-mismatch/source-json-5/full-snapshot-worldcities-json/2020/01/02/20200102_worldCities.json"

    val notExpectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/full-snapshot-worldcities-json/2020/01/02/20200102_worldCities.json.gz"

    val notExpectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database5/full_snapshot_worldcities_json"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch, None,
      None, Some(expectedSchemaMismatch), None, None,
      None, Some(9), Some(1), None, None,
      None)

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-json", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedBronzePath)))
    assert(Files.notExists(Paths.get(notExpectedSilverPath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileActivity#json ingestion full-snapshot permissive ok") {
    val filePath = "pending/source-json-5/20200101_permissive_worldCities.json"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatch = this.indationProperties.landing.basePath +
      "schema-mismatch/source-json-5/full-snapshot-worldcities-permissive-json/2020/01/01/20200101_permissive_worldCities.csv.bz2"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/full-snapshot-worldcities-permissive-json/2020/01/01/20200101_permissive_worldCities.json.gz"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database5/full_snapshot_worldcities_permissive_json"

    val bronzeDF = spark.read.format("json").load(expectedBronzePath)
    val silverDF = spark.sql("select * from database5.full_snapshot_worldcities_permissive_json")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors, None,
      None, Some(expectedSchemaMismatch), None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(9), Some(1), Some(9), Some(0),
      Some(ActivitySilverPersistence(Some("database5"), Some("full_snapshot_worldcities_permissive_json"), None, Some(0L),
        Some("full_snapshot_worldcities_permissive_json_historical"), None, Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-permissive-json", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedSchemaMismatch)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 10)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 9)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileActivity#json ingestion full-snapshot permissive ko") {
    val filePath = "pending/source-json-5/20200102_permissive_worldCities.json"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatch = this.indationProperties.landing.basePath +
      "schema-mismatch/source-json-5/full-snapshot-worldcities-permissive-json/2020/01/02/20200102_permissive_worldCities.json"

    val notExpectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/full-snapshot-worldcities-permissive-json/2020/01/02/20200102_permissive_worldCities.json.gz"

    val notExpectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database5/full_snapshot_worldcities_permissive_json"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch, None,
      None, Some(expectedSchemaMismatch), None, None,
      None, Some(8), Some(2), None, None,
      None)

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-permissive-json", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedSchemaMismatch)))
    assert(Files.notExists(Paths.get(notExpectedBronzePath)))
    assert(Files.notExists(Paths.get(notExpectedSilverPath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileActivity# json ingestion last changes fail fast ok 2 files") {
    val filePath = "pending/source-json-5/20200101_last_changes_worldCities.json"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-json-5/20200102_last_changes_worldCities.json"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/last-changes-worldcities-json/2020/01/01/20200101_last_changes_worldCities.json.gz"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source-json-5/last-changes-worldcities-json/2020/01/02/20200102_last_changes_worldCities.json.gz"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database5/last_changes_worldcities_json"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database5/last_changes_worldcities_json" +
      historicalSuffix

    val bronzeDF = spark.read.json(expectedBronzePath)
    val bronzeDF2 = spark.read.json(expectedBronzePath2)
    val silverDF = spark.sql("select * from database5.last_changes_worldcities_json")
    val silverDFHistorical = spark.sql("select * from database5.last_changes_worldcities_json" + historicalSuffix)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head //stats.head
    val activityStatistics2 = stats.head //stats.tail.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database5"), Some("last_changes_worldcities_json"), None, Some(0L), Some
      ("last_changes_worldcities_json_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database5"), Some("last_changes_worldcities_json"), None, Some(0L), Some
      ("last_changes_worldcities_json_historical"),
        Some(0L), Some(1L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "last-changes-worldcities-json", IngestionTypes.LastChanges, ValidationTypes.FailFast)
    assertActivityDataset(activityStatistics2, "last-changes-worldcities-json", IngestionTypes.LastChanges, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 3)
    assert(Files.exists(Paths.get(expectedBronzePath2)))
    assert(bronzeDF2.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 4)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
    assert(silverDFHistorical.count() == 6)
    assertInfoColumns(silverDF)
    assertInfoColumns(silverDFHistorical)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))

    val spain = "Spain"
    val silverSchema = List(
      ("name", StringType, true),
      ("country", StringType, true),
      ("subcountry", StringType, true),
      ("geonameid", LongType, true),
      ("timeStamp", TimestampType, true),
      ("yyyy", StringType, true),
      ("mm", StringType, true),
      ("dd", StringType, true)
    )

    val silverTable = spark.createDF(
      List(
        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02"),
        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02"),
        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02"),
        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
      silverHistoricalTable, orderedComparison = false)
  }
}
