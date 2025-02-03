package com.minsait.indation.activity

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.BuildInfo
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.indation.activity.exceptions.IngestionException
import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
import com.minsait.indation.metadata.MetadataFilesManager
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

class IngestBatchFileXlsActivitySpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with BeforeAndAfterEach
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
    , SecurityProperties(SecurityIdentityProperties("", "", ""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  private val fs: FSCommands = FSCommandsFactory.getFSCommands(indationProperties.environment)

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

  test("IngestBatchFileXlsActivity# excel ingestion full-snapshot sheet and data range specified without header all ok") {

    val filePath = "pending/source-excel-6/20200101_sheet_datarange_worldCities.xlsx"
    val nombreHoja = "Hoja2"
    val posicionInicial = "A3"

    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-sheet-datarange-worldcities/2020/01/01/20200101_sheet_datarange_worldCities.xlsx"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/full-snapshot-sheet-datarange-worldcities/2020/01/01/20200101_sheet_datarange_worldCities.xlsx"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/full-snapshot-sheet-datarange-worldcities/2020/01/01/20200101_sheet_datarange_worldCities.xlsx.gz"
    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/full_snapshot_sheet_datarange_worldcities"

    val bronzeDF = spark.read
      .format("excel")
      .option("dataAddress", s"'${nombreHoja}'!${posicionInicial}")
      .option("header", "false")
      .load(expectedBronzePathUncompressed)
    val silverDF = spark.sql("select * from database6.full_snapshot_sheet_datarange_worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("full_snapshot_sheet_datarange_worldcities"), None, Some(0L), Some("full_snapshot_sheet_datarange_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-sheet-datarange-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion full-snapshot with only sheet specified ignore header all ok") {

    val filePath = "pending/source-excel-6/20200101_sheet_worldCities.xls"
    val nombreHoja = "Hoja2"

    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-sheet-worldcities/2020/01/01/20200101_sheet_worldCities.xls"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/full-snapshot-sheet-worldcities/2020/01/01/20200101_sheet_worldCities.xls"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/full-snapshot-sheet-worldcities/2020/01/01/20200101_sheet_worldCities.xls.gz"
    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/full_snapshot_sheet_worldcities"

    val bronzeDF = spark.read
      .format("excel")
      .option("dataAddress", s"'${nombreHoja}'!A1")
      .option("header", "true")
      .load(expectedBronzePathUncompressed)
    val silverDF = spark.sql("select * from database6.full_snapshot_sheet_worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("full_snapshot_sheet_worldcities"), None, Some(0L), Some("full_snapshot_sheet_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-sheet-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion full-snapshot with only datarange specified all ok") {

    val filePath = "pending/source-excel-6/20200101_datarange_worldCities.xls"
    val posicionInicial = "A1"

    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-datarange-worldcities/2020/01/01/20200101_datarange_worldCities.xls"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/full-snapshot-datarange-worldcities/2020/01/01/20200101_datarange_worldCities.xls"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/full-snapshot-datarange-worldcities/2020/01/01/20200101_datarange_worldCities.xls.gz"
    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/full_snapshot_datarange_worldcities"

    val bronzeDF = spark.read
      .format("excel")
      .option("dataAddress", s"${posicionInicial}")
      .option("header", "true")
      .load(expectedBronzePathUncompressed)
    val silverDF = spark.sql("select * from database6.full_snapshot_datarange_worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("full_snapshot_datarange_worldcities"), None, Some(0L), Some("full_snapshot_datarange_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-datarange-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion full-snapshot no sheet and no data range specified all ok") {

    val filePath = "pending/source-excel-6/20200101_worldCities.xlsx"

    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-worldcities/2020/01/01/20200101_worldCities.xlsx"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/full-snapshot-worldcities/2020/01/01/20200101_worldCities.xlsx"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/full-snapshot-worldcities/2020/01/01/20200101_worldCities.xlsx.gz"
    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/full_snapshot_worldcities"

    val bronzeDF = spark.read.option("header", "true")
      .format("excel")
      .option("header", "true")
      .load(expectedBronzePathUncompressed)
    val silverDF = spark.sql("select * from database6.full_snapshot_worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("full_snapshot_worldcities"), None, Some(0L), Some("full_snapshot_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion wrong column order schema-mismatch fail-fast") {

    val filePath = "pending/source-excel-6/20200101_mismatch_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-mismatch-worldcities/2020/01/01/20200101_mismatch_worldCities.xlsx"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch, None,
      None, Some(expectedSchemaMismatchPath), None, None,
      None, Some(0), Some(0), None, None, None)

    assertActivityDataset(activityStatistics, "full-snapshot-mismatch-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion schema-mismatch permissive absolute ko") {
    val filePath = "pending/source-excel-6/20200201_permissive_mismatch_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-permissive-mismatch-worldcities/2020/02/01/20200201_permissive_mismatch_worldCities.xlsx"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch, None,
      None, Some(expectedSchemaMismatchPath), None, None,
      None, Some(22995), Some(23), None, None, None)

    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-permissive-mismatch-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion schema-mismatch permissive absolute ok") {
    val filePath = "pending/source-excel-6/20200101_permissive_mismatch_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/full-snapshot-permissive-mismatch-worldcities/2020/01/01/20200101_permissive_mismatch_worldCities.csv.bz2"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/full-snapshot-permissive-mismatch-worldcities/2020/01/01/20200101_permissive_mismatch_worldCities.xlsx"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/full-snapshot-permissive-mismatch-worldcities/2020/01/01/20200101_permissive_mismatch_worldCities.xlsx.gz"
    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/full_snapshot_permissive_mismatch_worldcities"

    val shemaMismatchDF = spark.read.csv(expectedSchemaMismatchPath)
    val bronzeDF = spark.read.option("header", "true")
      .format("excel")
      .option("header", "true")
      .load(expectedBronzePathUncompressed)
    val silverDF = spark.sql("select * from database6.full_snapshot_permissive_mismatch_worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors, None,
      None, Some(expectedSchemaMismatchPath), None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23016), Some(2), Some(23016), Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("full_snapshot_permissive_mismatch_worldcities"), None, Some(0L), Some("full_snapshot_permissive_mismatch_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-permissive-mismatch-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
    assert(shemaMismatchDF.count() == 2)
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23016)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileXlsActivity# excel ingestion incremental schema evolution") {
    val filePath = "pending/source-excel-6/20200103_incremental_worldCities.xls"
    val posicionInicial = "A1"

    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-excel-6/20200104_incremental_evo_worldCities.xls"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/incremental-worldcities/2020/01/03/20200103_incremental_worldCities.xls.gz"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/incremental-worldcities/2020/01/04/20200104_incremental_evo_worldCities.xls.gz"
    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedBronzePath2Uncompressed = this.fs.uncompressGzip(expectedBronzePath2)

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/incremental_worldcities"

    val bronzeDF = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"${posicionInicial}")
      .load(expectedBronzePathUncompressed)
    val bronzeDF2 = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"${posicionInicial}")
      .load(expectedBronzePath2Uncompressed)
    val silverDF = spark.sql("select * from database6.incremental_worldcities")

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head//stats.head
    val activityStatistics2 = stats.head//stats.tail.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(2), Some(0), Some(2),
      Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("incremental_worldcities"), None, Some(0L), None, None,
        None)))

    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(2), Some(0), Some(2),
      Some(0), Some(ActivitySilverPersistence(Some("database6"), Some("incremental_worldcities"), Some(0L), Some(1L), None, None,
        None)))

    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "incremental-worldcities", IngestionTypes.Incremental, ValidationTypes.Permissive)
    assertActivityDataset(activityStatistics2, "incremental-worldcities", IngestionTypes.Incremental, ValidationTypes.Permissive)


    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 2)
    assert(Files.exists(Paths.get(expectedBronzePath2)))
    assert(bronzeDF2.count() == 2)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 4)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))

    val silverSchema = List(
      ("name", StringType, true),
      ("country", StringType, true),
      ("subcountry", StringType, true),
      ("geonameid", LongType, true),
      ("timeStamp", TimestampType, true),
      ("yyyy", StringType, true),
      ("mm", StringType, true),
      ("dd", StringType, true),
      ("evo1", StringType, true),
      ("geonameid2", LongType, true)
    )

    val silverTable = spark.createDF(
      List(
        ("Alicante", "Spain", "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "03", null, null),
        ("Cáceres", "Spain", "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "03", null, null),
        ("Ciudad Real", "Spain", "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "04", "evo1", 1L),
        ("Tomelloso", "Spain", "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "04", "evo1", 2L)
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
  }

  test("IngestBatchFileXlsActivity# excel ingestion last changes fail fast ok 2 files") {
    val posicionInicial = "A2"
    val hoja = "Hoja1"
    val filePath = "pending/source-excel-6/20200101_last_changes_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-excel-6/20200102_last_changes_worldCities.xlsx"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-worldcities/2020/01/01/20200101_last_changes_worldCities.xlsx.gz"

    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-worldcities/2020/01/02/20200102_last_changes_worldCities.xlsx.gz"

    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedBronzePath2Uncompressed = this.fs.uncompressGzip(expectedBronzePath2)

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_worldcities"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_worldcities" + historicalSuffix

    val bronzeDF = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedBronzePathUncompressed)
    val bronzeDF2 = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedBronzePath2Uncompressed)

    val silverDF = spark.sql("select * from database6.last_changes_worldcities")
    val silverDFHistorical = spark.sql("select * from database6.last_changes_worldcities" + historicalSuffix)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head//stats.head
    val activityStatistics2 = stats.head//stats.tail.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("last_changes_worldcities"), None, Some(0L), Some("last_changes_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("last_changes_worldcities"), None, Some(0L), Some("last_changes_worldcities_historical"),
        Some(0L), Some(1L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "last-changes-worldcities", IngestionTypes.LastChanges, ValidationTypes.FailFast)
    assertActivityDataset(activityStatistics2, "last-changes-worldcities", IngestionTypes.LastChanges, ValidationTypes.FailFast)

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

  test("IngestBatchFileXlsActivity# excel ingestion last changes with sheet and datarange specified and permissiveThresholdType percentage 50 KO") {
    val posicionInicial = "A2"
    val hoja = "Hoja1"
    val filePath = "pending/source-excel-6/20200101_last_changes_permissive_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-excel-6/20200102_last_changes_permissive_worldCities.xlsx"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-permissive-worldcities/2020/01/01/20200101_last_changes_permissive_worldCities.xlsx.gz"

    val notExpectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-permissive-worldcities/2020/01/02/20200102_last_changes_permissive_worldCities.xlsx.gz"

    val expectedInvalidPath2 = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/last-changes-permissive-worldcities/2020/01/02/20200102_last_changes_permissive_worldCities.xlsx"

    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    //val expectedBronzePath2Uncompressed = this.fs.uncompressGzip(expectedBronzePath2)

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_permissive_worldcities"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_permissive_worldcities" + historicalSuffix

    val bronzeDF = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedBronzePathUncompressed)

    val silverDF = spark.sql("select * from database6.last_changes_permissive_worldcities")
    val silverDFHistorical = spark.sql("select * from database6.last_changes_permissive_worldcities" + historicalSuffix)
    val invalidDF2 = spark.read
      .format("excel")
      .option("header", "true")
      //.option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedInvalidPath2)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head//stats.head
    val activityStatistics2 = stats.head//stats.tail.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("last_changes_permissive_worldcities"), None, Some(0L), Some("last_changes_permissive_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.RejectedInvalid, None,
      None, None, Some(expectedInvalidPath2), None,
      None, Some(3), Some(0), Some(1), Some(2), None)

    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "last-changes-permissive-worldcities", IngestionTypes.LastChanges, ValidationTypes.Permissive)
    assertActivityDataset(activityStatistics2, "last-changes-permissive-worldcities", IngestionTypes.LastChanges, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedInvalidPath2)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 3)
    assert(Files.notExists(Paths.get(notExpectedBronzePath2)))
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
    assert(silverDFHistorical.count() == 3)
    assertInfoColumns(silverDF)
    assertInfoColumns(silverDFHistorical)
    assert(Files.exists(Paths.get(expectedInvalidPath2)))
    assert(invalidDF2.count() == 3)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))

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
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
      silverHistoricalTable, orderedComparison = false)
  }

  test("IngestBatchFileXlsActivity# excel ingestion last changes with sheet and datarange specified and permissiveThresholdType percentage 50 OK") {
    val posicionInicial = "A2"
    val hoja = "Hoja1"
    val filePath = "pending/source-excel-6/20200201_last_changes_permissive_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-excel-6/20200203_last_changes_permissive_worldCities.xlsx"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-permissive-worldcities/2020/02/01/20200201_last_changes_permissive_worldCities.xlsx.gz"

    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-permissive-worldcities/2020/02/03/20200203_last_changes_permissive_worldCities.xlsx.gz"

    val expectedInvalidPath2 = this.indationProperties.landing.basePath +
      "invalid/source-excel-6/last-changes-permissive-worldcities/2020/02/03/20200203_last_changes_permissive_worldCities.csv.bz2"

    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    val expectedBronzePath2Uncompressed = this.fs.uncompressGzip(expectedBronzePath2)

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_permissive_worldcities"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_permissive_worldcities" + historicalSuffix

    val bronzeDF = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedBronzePathUncompressed)
    val bronzeDF2 = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedBronzePath2Uncompressed)

    val silverDF = spark.sql("select * from database6.last_changes_permissive_worldcities")
    val silverDFHistorical = spark.sql("select * from database6.last_changes_permissive_worldcities" + historicalSuffix)
    val invalidDF2 = spark.read.csv(expectedInvalidPath2)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head//stats.head
    val activityStatistics2 = stats.head//stats.tail.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("last_changes_permissive_worldcities"), None, Some(0L), Some("last_changes_permissive_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_With_Errors, None,
      None, None, Some(expectedInvalidPath2), Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(2),
      Some(1),
      Some(ActivitySilverPersistence(Some("database6"), Some("last_changes_permissive_worldcities"), None, Some(0L), Some("last_changes_permissive_worldcities_historical"), Some(0L),
        Some(1L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "last-changes-permissive-worldcities", IngestionTypes.LastChanges, ValidationTypes.Permissive)
    assertActivityDataset(activityStatistics2, "last-changes-permissive-worldcities", IngestionTypes.LastChanges, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedInvalidPath2)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 3)
    assert(Files.exists(Paths.get(expectedBronzePath2)))
    assert(bronzeDF2.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 4)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
    assert(silverDFHistorical.count() == 5)
    assertInfoColumns(silverDF)
    assertInfoColumns(silverDFHistorical)
    assert(Files.exists(Paths.get(expectedInvalidPath2)))
    assert(invalidDF2.count() == 1)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))

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
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-02-01 01:00:00"), "2020", "02", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-02-01 01:00:00"), "2020", "02", "01"),
        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-02-03 01:00:00"), "2020", "02", "03"),
        ("Tomelloso", spain, "Castilla La Mancha", 3245678L, Timestamp.valueOf("2020-02-03 01:00:00"), "2020", "02", "03")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-02-01 01:00:00"), "2020", "02", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-02-01 01:00:00"), "2020", "02", "01"),
        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-02-01 01:00:00"), "2020", "02", "01"),
        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-02-03 01:00:00"), "2020", "02", "03"),
        ("Tomelloso", spain, "Castilla La Mancha", 3245678L, Timestamp.valueOf("2020-02-03 01:00:00"), "2020", "02", "03")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
      silverHistoricalTable, orderedComparison = false)
  }

  test("IngestBatchFileXlsActivity# excel ingestion last changes with sheet and datarange specified and permissiveThresholdType percentage 50 schema mismatch KO") {
    val posicionInicial = "A2"
    val hoja = "Hoja1"
    val filePath = "pending/source-excel-6/20200301_last_changes_permissive_worldCities.xlsx"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-excel-6/20200304_last_changes_permissive_worldCities.xlsx"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-permissive-worldcities/2020/03/01/20200301_last_changes_permissive_worldCities.xlsx.gz"

    val notExpectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source-excel-6/last-changes-permissive-worldcities/2020/03/04/20200304_last_changes_permissive_worldCities.xlsx.gz"

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source-excel-6/last-changes-permissive-worldcities/2020/03/04/20200304_last_changes_permissive_worldCities.xlsx"

    val expectedBronzePathUncompressed = this.fs.uncompressGzip(expectedBronzePath)
    //val expectedBronzePath2Uncompressed = this.fs.uncompressGzip(notExpectedBronzePath2)

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_permissive_worldcities"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database6/last_changes_permissive_worldcities" + historicalSuffix

    val bronzeDF = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", s"'${hoja}'!${posicionInicial}")
      .load(expectedBronzePathUncompressed)

    val silverDF = spark.sql("select * from database6.last_changes_permissive_worldcities")
    val silverDFHistorical = spark.sql("select * from database6.last_changes_permissive_worldcities" + historicalSuffix)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head//stats.head
    val activityStatistics2 = stats.head//stats.tail.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database6"), Some("last_changes_permissive_worldcities"), None, Some(0L), Some("last_changes_permissive_worldcities_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.RejectedSchemaMismatch, None,
      None, Some(expectedSchemaMismatchPath), None, None,
      None, Some(1), Some(2), None, None, None)

    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "last-changes-permissive-worldcities", IngestionTypes.LastChanges, ValidationTypes.Permissive)
    assertActivityDataset(activityStatistics2, "last-changes-permissive-worldcities", IngestionTypes.LastChanges, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 3)
    assert(Files.notExists(Paths.get(notExpectedBronzePath2)))
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
    assert(silverDFHistorical.count() == 3)
    assertInfoColumns(silverDF)
    assertInfoColumns(silverDFHistorical)
    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))

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
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-03-01 01:00:00"), "2020", "03", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-03-01 01:00:00"), "2020", "03", "01"),
        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-03-01 01:00:00"), "2020", "03", "01")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-03-01 01:00:00"), "2020", "03", "01"),
        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-03-01 01:00:00"), "2020", "03", "01"),
        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-03-01 01:00:00"), "2020", "03", "01")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
      silverHistoricalTable, orderedComparison = false)
  }
}
