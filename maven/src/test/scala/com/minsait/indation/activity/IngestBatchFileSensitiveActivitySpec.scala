package com.minsait.indation.activity

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.EncryptUtils._
import com.minsait.common.utils.{BuildInfo, EncryptUtils}
import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, ValidationTypes}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.enrichString

import java.io._
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import scala.util.Random

class IngestBatchFileSensitiveActivitySpec extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach
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
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties(SecurityEncryptionTypes.Pseudonymization,
      Some("mLtzjLyHZaUzBN34hYuSwSWuK/Drbbw+EuZeFnIzoeA="), Some(true),
      Some(EncryptionAlgorithm(EncryptionAlgorithmTypes.Aes, EncryptionModeTypes.Ctr, "NoPadding")), Some("GXCQ9QgX4QZ4i7K")))
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
  private var map: Map[String,String] = Map()

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
    EncryptUtils.setProperties(indationProperties)
    map = Map(
      "algorithm" -> EncryptUtils.algorithm,
      "algorithm_transformation" -> EncryptUtils.algorithm_transformation,
      "keySalt" -> EncryptUtils.keySalt,
      "salt" -> EncryptUtils.salt
    )
    spark.sparkContext.broadcast(map)
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

  test("IngestBatchFileActivity# csv ingestion NO sensitive fields rawMasking true OK") {
    val filePath = "pending/source1/20200101_worldCities_no_sensitive.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/full-snapshot-worldcities-no-sensitive/2020/01/01/20200101_worldCities_no_sensitive.csv.bz2"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source1/full-snapshot-worldcities-no-sensitive/2020/01/01/20200101_worldCities_no_sensitive.csv"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source1/full-snapshot-worldcities-no-sensitive/2020/01/01/20200101_worldCities_no_sensitive.csv.gz"
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities_no_sensitive"

    val bronzeDF = spark.read.option("header", "true").csv(expectedBronzePath)
    val silverDF = spark.sql("select * from database1.full_snapshot_worldcities_no_sensitive")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database1"), Some("full_snapshot_worldcities_no_sensitive"), None, Some(0L), Some("full_snapshot_worldcities_no_sensitive_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-no-sensitive", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
  }

  test("IngestBatchFileSensitiveActivity# csv ingestion full snapshot sensitive columns rawMasking false") {
    val indationProperties: IndationProperties = IndationProperties(
      MetadataProperties("", testResourcesBasePath, "", "")
      , Some(DatabricksProperties(DatabricksSecretsProperties("")))
      , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
        , "invalid", "corrupted", "schema-mismatch", "streaming")
      , DatalakeProperties("", testResourcesBasePath, "")
      , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties(SecurityEncryptionTypes.Pseudonymization, Some("mLtzjLyHZaUzBN34hYuSwSWuK/Drbbw+EuZeFnIzoeA="), Some(false),
        Some(EncryptionAlgorithm(EncryptionAlgorithmTypes.Aes, EncryptionModeTypes.Ctr, "NoPadding")), Some("GXCQ9QgX4QZ4i7K")))
      , Local,
      tmpDirectory
    )

    val filePath = "pending/source1/20200101_worldCities_sensitive.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/full-snapshot-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive.csv.bz2"
    val notExpectedSchemaMismatchPath_sensitive = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/full-snapshot-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive_sensitive.csv.bz2"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source1/full-snapshot-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive.csv"
    val notExpectedInvalidPath_sensitive = this.indationProperties.landing.basePath +
      "invalid/source1/full-snapshot-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive_sensitive.csv"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source1/full-snapshot-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive.csv.gz"
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities_sensitive"
    val expectedSilverPath_sensitive = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities_sensitive_sensitive"

    val bronzeDF = spark.read.option("header", "true").csv(expectedBronzePath)
    val silverDF = spark.sql("select * from database1.full_snapshot_worldcities_sensitive")
    val silverDF_sensitive = spark.sql("select * from database1.full_snapshot_worldcities_sensitive_sensitive")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
      Some(ActivitySilverPersistence(Some("database1"), Some("full_snapshot_worldcities_sensitive"), None, Some(0L), Some("full_snapshot_worldcities_sensitive_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "full-snapshot-worldcities-sensitive", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 23018)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 23018)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath_sensitive)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath_sensitive)))
    assert(Files.exists(Paths.get(expectedSilverPath_sensitive)))
    assert(silverDF_sensitive.count() == 23018)
    assertInfoColumns(silverDF_sensitive)

    val id = silverDF.select("_id")
    val id_sensitive = silverDF_sensitive.select(col("_id"))

    assertSmallDatasetEquality(id, id_sensitive, orderedComparison = false)

    val masterKey = this.indationProperties.getSecret(this.indationProperties.security.encryption.masterKey.get)
    val originalBronzeDF = bronzeDF.select("name", "geonameid")
    val decryptedSilverDF = silverDF_sensitive
      .withColumn("decrypted_key", decryptUDF(masterKey,map)(col("encrypted_key")) )
      .withColumn("decrypted_name", decryptUDF(map)(col("decrypted_key"), col("encrypted_name")))
      .withColumn("decrypted_geonameid", decryptUDF(map)(col("decrypted_key"), col("encrypted_geonameid_alias")))
      .select(col("decrypted_name").as("name"), col("decrypted_geonameid").as("geonameid"))

    assertSmallDatasetEquality(originalBronzeDF, decryptedSilverDF, orderedComparison = false)
  }

  test("IngestBatchFileSensitiveActivity# csv ingestion last changes sensitive columns primary key sensitive rawMasking true") {
    val filePath = "pending/source1/20200101_last_changes_worldCities_sensitive.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)
    val filePath2 = "pending/source1/20200102_last_changes_worldCities_sensitive.csv"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/last-changes-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive.csv.bz2"
    val notExpectedSchemaMismatchPath_sensitive = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/last-changes-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive_sensitive.csv.bz2"

    val notExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source1/last-changes-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive.csv"
    val notExpectedInvalidPath_sensitive = this.indationProperties.landing.basePath +
      "invalid/source1/last-changes-worldcities-sensitive/2020/01/01/20200101_worldCities_sensitive_sensitive.csv"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source1/last-changes-worldcities-sensitive/2020/01/01/encrypted_20200101_last_changes_worldCities_sensitive.csv.gz"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source1/last-changes-worldcities-sensitive/2020/01/02/encrypted_20200102_last_changes_worldCities_sensitive.csv.gz"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities_sensitive"
    val expectedSilverPath_sensitive = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities_sensitive_sensitive"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities_sensitive" + historicalSuffix
    val expectedSilverPathHistorical_sensitive = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities_sensitive_sensitive" + historicalSuffix

    val bronzeDF = spark.read.option("header", "true").csv(decryptFile(expectedBronzePath))
    val bronzeDF2 = spark.read.option("header", "true").csv(decryptFile(expectedBronzePath2))
    val silverDF = spark.sql("select * from database1.last_changes_worldcities_sensitive")
    val silverDF_sensitive = spark.sql("select * from database1.last_changes_worldcities_sensitive_sensitive")
    val silverDFHistorical = spark.sql("select * from database1.last_changes_worldcities_sensitive" + historicalSuffix)
    val silverDFHistorical_sensitive = spark.sql("select * from database1.last_changes_worldcities_sensitive_sensitive" + historicalSuffix)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head
    val activityStatistics2 = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database1"), Some("last_changes_worldcities_sensitive"), None, Some(0L), Some("last_changes_worldcities_sensitive_historical"), None,
        Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0),
      Some(ActivitySilverPersistence(Some("database1"), Some("last_changes_worldcities_sensitive"), None, Some(0L), Some("last_changes_worldcities_sensitive_historical"),
        Some(0L), Some(1L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "last-changes-worldcities-sensitive", IngestionTypes.LastChanges, ValidationTypes.FailFast)
    assertActivityDataset(activityStatistics2, "last-changes-worldcities-sensitive", IngestionTypes.LastChanges, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath_sensitive)))
    assert(Files.notExists(Paths.get(notExpectedInvalidPath_sensitive)))

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


    assert(Files.exists(Paths.get(expectedSilverPath_sensitive)))
    assert(silverDF_sensitive.count() == 4)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical_sensitive)))
    assert(silverDFHistorical.count() == 6)
    assertInfoColumns(silverDF_sensitive)
    assertInfoColumns(silverDFHistorical_sensitive)

    val id = silverDF.select("_id")
    val id_sensitive = silverDF_sensitive.select(col("_id"))

    assertSmallDatasetEquality(id, id_sensitive, orderedComparison = false)

    val masterKey = this.indationProperties.getSecret(this.indationProperties.security.encryption.masterKey.get)

    val schema = List(
      ("name", StringType, true),
      ("geonameid", StringType, true)
    )

    val dataframe = spark.createDF(
      List(
        ("Alicante", "2521978"),
        ("Cáceres", "2520611"),
        ("Ciudad Real", "2519402"),
        ("Tomelloso", "2510392")
      ),
      schema
    )

    val expectedDecryptedColumnsDF = dataframe
    val decryptedSilverDF = silverDF_sensitive
      .withColumn("decrypted_key", decryptUDF(masterKey,map)(col("encrypted_key")) )
      .withColumn("decrypted_name", decryptUDF(map)(col("decrypted_key"), col("encrypted_name")))
      .withColumn("decrypted_geonameid", decryptUDF(map)(col("decrypted_key"), col("encrypted_geonameid")))
      .select(col("decrypted_name").as("name"), col("decrypted_geonameid").as("geonameid"))

    assertSmallDatasetEquality(expectedDecryptedColumnsDF, decryptedSilverDF, orderedComparison = false)
  }

  test("IngestBatchFileSensitiveActivity# csv ingestion incremental sensitive columns null values rawMasking true") {

    val filePath = "pending/source1/20200101_incremental_worldCities_sensitive.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source1/20200102_incremental_worldCities_sensitive.csv"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val nonExpectedSchemaMismatchPath = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/incremental-worldcities-sensitive/2020/01/01/20200101_incremental_worldCities_sensitive.csv.bz2"
    val nonExpectedSchemaMismatchPath2 = this.indationProperties.landing.basePath +
      "schema-mismatch/source1/incremental-worldcities-sensitive/2020/01/02/20200102_incremental_worldCities_sensitive.csv.bz2"

    val nonExpectedInvalidPath = this.indationProperties.landing.basePath +
      "invalid/source1/incremental-worldcities-sensitive/2020/01/01/20200101_incremental_worldCities_sensitive.csv.bz2"
    val nonExpectedInvalidPath2 = this.indationProperties.landing.basePath +
      "invalid/source1/incremental-worldcities-sensitive/2020/01/02/20200102_incremental_worldCities_sensitive.csv.bz2"

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      "bronze/source1/incremental-worldcities-sensitive/2020/01/01/encrypted_20200101_incremental_worldCities_sensitive.csv.gz"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      "bronze/source1/incremental-worldcities-sensitive/2020/01/02/encrypted_20200102_incremental_worldCities_sensitive.csv.gz"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/incremental_worldcities_sensitive"
    val expectedSilverPath_sensitive = this.indationProperties.datalake.basePath + "silver/public/database1/incremental_worldcities_sensitive_sensitive"


    val bronzeDF = spark.read.option("header", "true").csv(decryptFile(expectedBronzePath))
    val bronzeDF2 = spark.read.option("header", "true").csv(decryptFile(expectedBronzePath2))
    val silverDF = spark.sql("select * from database1.incremental_worldcities_sensitive")
    val silverDF_sensitive = spark.sql("select * from database1.incremental_worldcities_sensitive_sensitive")

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head
    val activityStatistics2 = stats.head

    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(17), Some(0), Some(17), Some(0),
      Some(ActivitySilverPersistence(Some("database1"), Some("incremental_worldcities_sensitive"), None, Some(0L), None, None,
        None)))

    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2),
      Some(expectedSilverPath), Some(16), Some(0), Some(16), Some(0),
      Some(ActivitySilverPersistence(Some("database1"), Some("incremental_worldcities_sensitive"), Some(0L), Some(1L), None, None,
        None)))

    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "incremental-worldcities-sensitive", IngestionTypes.Incremental, ValidationTypes.Permissive)
    assertActivityDataset(activityStatistics2, "incremental-worldcities-sensitive", IngestionTypes.Incremental, ValidationTypes.Permissive)

    assert(Files.notExists(Paths.get(nonExpectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(nonExpectedInvalidPath)))
    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 17)
    assert(Files.notExists(Paths.get(nonExpectedSchemaMismatchPath2)))
    assert(Files.notExists(Paths.get(nonExpectedInvalidPath2)))
    assert(Files.exists(Paths.get(expectedBronzePath2)))
    assert(bronzeDF2.count() == 16)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 33)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))

    assert(Files.exists(Paths.get(expectedSilverPath_sensitive)))
    assert(silverDF_sensitive.count() == 33)
    assertInfoColumns(silverDF_sensitive)

    val id = silverDF.select("_id")
    val id_sensitive = silverDF_sensitive.select(("_id"))
    assertSmallDatasetEquality(id, id_sensitive, orderedComparison = false)

    val masterKey = this.indationProperties.getSecret(this.indationProperties.security.encryption.masterKey.get)
    val originalBronzeDF = bronzeDF.select("name", "geonameid")
      .union(bronzeDF2.select("name", "geonameid"))

    val decryptedSilverDF = silverDF_sensitive
      .withColumn("decrypted_key", decryptUDF(masterKey,map)(col("encrypted_key")) )
      .withColumn("decrypted_name", decryptUDF(map)(col("decrypted_key"), col("encrypted_name")))
      .withColumn("decrypted_geonameid", decryptUDF(map)(col("decrypted_key"), col("encrypted_geonameid")))
      .select(col("decrypted_name").as("name"), col("decrypted_geonameid").as("geonameid"))
      .withColumn("name", when(col("name").equalTo("null"), lit(null).cast(StringType)).otherwise(col("name")))
      .withColumn("geonameid", when(col("geonameid").equalTo("null"), lit(null).cast(StringType)).otherwise(col("geonameid")))

    assertSmallDatasetEquality(originalBronzeDF, decryptedSilverDF, orderedComparison = false)
  }
}
