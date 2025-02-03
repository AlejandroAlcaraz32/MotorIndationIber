package com.minsait.indation.activity

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.EncryptUtils.{decryptFile, encryptFile}
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.common.utils.{BuildInfo, EncryptUtils, FileNameUtils}
import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
import com.minsait.indation.datalake.readers.LandingApiReader
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.models.enums.{ColumnsTypes, DatasetTypes, IngestionTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Column, Dataset, SchemaColumns, Source}
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.enrichString

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

class IngestApiActivitySpec extends AnyFunSuite with MockitoSugar  with BeforeAndAfterAll with BeforeAndAfterEach
  with DatasetComparer with SparkSessionWrapper {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\",DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val dataLakeStorageDate = "datalake_load_date"
  private val dataLakeIngestionUuid = "datalake_ingestion_uuid"
  private val dataLakeStorageDay = "datalake_load_day"
  private val yyyy = "yyyy"
  private val mm = "mm"
  private val dd = "dd"
  private val adfRunId = "adf-uuid"
  private val historicalSuffix = "_historical"

  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("",testResourcesBasePath,"","")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("",testResourcesBasePath,"")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties(SecurityEncryptionTypes.Pseudonymization,
      Some("mLtzjLyHZaUzBN34hYuSwSWuK/Drbbw+EuZeFnIzoeA="), Some(true),
      Some(EncryptionAlgorithm(EncryptionAlgorithmTypes.Aes, EncryptionModeTypes.Ctr, "NoPadding")), Some("GXCQ9QgX4QZ4i7K")))
    , Local,
    tmpDirectory
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
    EncryptUtils.setProperties(indationProperties)
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
                                    validation: ValidationTypes.ValidationType,
                                    partitionBy: String = "",
                                   ): Unit = {
    assertResult(name)(activityStatistics.dataset.get.name)
    assertResult(DatasetTypes.Api)(activityStatistics.dataset.get.typ)
    assertResult(1)(activityStatistics.dataset.get.version)
    assertResult(ingestion)(activityStatistics.dataset.get.ingestion_mode)
    assertResult(validation)(activityStatistics.dataset.get.validation_mode)
    assertResult(partitionBy)(activityStatistics.dataset.get.partition_by)
  }

  test("IngestApiActivitySpec# ingestion full-snapshot schema-mismatch fail-fast KO") {
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-full-snapshot-fail-fast-users")
    val avroFilePath = this.testResourcesBasePath + "landing/streaming/tmp/20220102_dataset-api-full-snapshot-fail-fast-users.bz2.avro"

    val schema = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Gender", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val dataframe = spark.createDF(
      List(
        ("Pedro", null, "Pedro", "Jimenez", "M", "asdf"),
        ("Juan", 8L, "Juan", "Clavijo", "M", null)
      ),
      schema
    )

    dataframe
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe, avroFilePath))

    ingestApiCallActivity.execute(dataset.get,adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "schema-mismatch/source-api/dataset-api-full-snapshot-fail-fast-users/")

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
      dirs + "20220102_dataset-api-full-snapshot-fail-fast-users.bz2.avro"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath, ActivityResults.RejectedSchemaMismatch,None,
      None, Some(expectedSchemaMismatchPath), None, None,
      None, Some(1), Some(1), None, None, None)

    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-api-full-snapshot-fail-fast-users", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)) )
    assert(Files.notExists(Paths.get(avroFilePath)))
  }

  test("IngestApiActivitySpec# ingestion full-snapshot schema-mismatch permissive absolute KO") {
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-full-snapshot-permissive-users")
    val landingAvroFilePath = this.indationProperties.landing.basePath + "landing/streaming/tmp/20220101_dataset-api-full-snapshot-permissive-users.bz2.avro"

    val schema = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
      ("Gender", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val dataframe = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", "M", null),
        ("Juan", 8L, null, "Clavijo", "2022-01-01 00:00:00", "M", "incorrect"),
        ("Lucia", 8L, "Lucia", null, "2022-01-01 00:00:00", "F", "incorrect")
      ),
      schema
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

    dataframe
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(landingAvroFilePath)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe, landingAvroFilePath))
    ingestApiCallActivity.execute(dataset.get,adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "schema-mismatch/source-api/dataset-api-full-snapshot-permissive-users/")

    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
      dirs + "20220101_dataset-api-full-snapshot-permissive-users.bz2.avro"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, landingAvroFilePath, ActivityResults.RejectedSchemaMismatch, None,
      None, Some(expectedSchemaMismatchPath), None, None,
      None, Some(1), Some(2), None, None, None)

    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-api-full-snapshot-permissive-users", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)

    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
    assert(Files.notExists(Paths.get(landingAvroFilePath)))
  }

  test("IngestApiActivitySpec# ingestion full-snapshot, all OK") {
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-full-snapshot-fail-fast-users")
    val avroFilePath = this.testResourcesBasePath + "landing/streaming/tmp/20220101_dataset-api-full-snapshot-fail-fast-users.bz2.avro"

    val schema = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Gender", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val dataframe = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "F", null),
        ("Juan", 8L, "Juan", "Clavijo", "M", null)
      ),
      schema
    )

    dataframe
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe, avroFilePath))

    ingestApiCallActivity.execute(dataset.get,adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-api/dataset-api-full-snapshot-fail-fast-users/")

    val expectedBronzePath = this.indationProperties.datalake.basePath +
      dirs + "20220101_dataset-api-full-snapshot-fail-fast-users.bz2.avro"
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database_api/full_snapshot_fail_fast_users"
    val bronzeDF = spark.read.format("avro").load(expectedBronzePath)
    val silverDF = spark.sql("select * from database_api.full_snapshot_fail_fast_users")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath, ActivityResults.Ingested_Without_Errors,None,
      None, None, None, Some(expectedBronzePath),
      Some(expectedSilverPath), Some(2), Some(0), Some(2), Some(0),
      Some(ActivitySilverPersistence(Some("database_api"),Some("full_snapshot_fail_fast_users"),None,Some(0L),
        Some("full_snapshot_fail_fast_users" + historicalSuffix),None,Some(0L))))

    assertActivityDataset(activityStatistics, "dataset-api-full-snapshot-fail-fast-users", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePath)))
    assert(bronzeDF.count() == 2)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 2)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(avroFilePath)))
  }

  test("IngestApiActivitySpec# incremental schema evolution partitionBy specified OK") {
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-incremental-users")

    val columnsEvo = dataset.get.schemaColumns.get.columns :+ Column(name = "Gender", typ = ColumnsTypes.String, description = Some(""),
      comment = Some(""), isPrimaryKey = None, isTimestamp = None, isPartitionable = None, transformation = None, decimalParameters = None, sensitive = false)

    val schemaColumnsEvo = SchemaColumns(columnsEvo)

    val datasetEvo = Some(Dataset(dataset.get.datasetId, dataset.get.sourceId, dataset.get.specificationId, dataset.get.name,
      dataset.get.description, dataset.get.sourceName, dataset.get.typ, dataset.get.fileInput, dataset.get.kafkaInput,
      dataset.get.tableInput, dataset.get.apiInput, dataset.get.version, dataset.get.enabled, dataset.get.effectiveDate,
      dataset.get.createDatabase, dataset.get.classification, dataset.get.partitionBy, dataset.get.allowPartitionChange,
      dataset.get.ingestionMode, dataset.get.database, dataset.get.table, dataset.get.validationMode, dataset.get.permissiveThreshold,
      dataset.get.permissiveThresholdType, dataset.get.schemaDefinition, Some(schemaColumnsEvo), dataset.get.schemaJson,
      dataset.get.qualityRules))

    val schema = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val schemaEvo = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
      ("Gender", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val dataframe1 = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", null),
        ("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00", null)
      ),
      schema
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    val dataframe2 = spark.createDF(
      List(
        ("Cristian", 15L, "Cristian", "Morillas", "2022-01-02 01:00:00", "M", null),
        ("Maria", 21L, "Maria", "", "2022-01-02 01:00:00", "F", null),
        ("Maria", 17L, "Maria", "Ramirez", "2022-01-02 01:00:00", "F", null)
      ),
      schemaEvo
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    dataframe1
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(this.testResourcesBasePath + "landing/streaming/tmp/20220101_dataset-api-incremental-users.bz2.avro")

    dataframe2
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(this.testResourcesBasePath + "landing/streaming/tmp/20220102_dataset-api-incremental-users.bz2.avro")

    val avroFilePath1 = this.testResourcesBasePath + "landing/streaming/tmp/20220101_dataset-api-incremental-users.bz2.avro"
    val avroFilePath2 = this.testResourcesBasePath + "landing/streaming/tmp/20220102_dataset-api-incremental-users.bz2.avro"
    //val dataframe = spark.read.format("avro").load(avroFilePath)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe1, avroFilePath1))
    ingestApiCallActivity.execute(dataset.get,adfRunId)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe2, avroFilePath2))
    ingestApiCallActivity.execute(datasetEvo.get,adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-api/dataset-api-incremental-users/")

    val expectedBronzePath1 = this.indationProperties.datalake.basePath +
      dirs + "20220101_dataset-api-incremental-users.bz2.avro"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      dirs + "20220102_dataset-api-incremental-users.bz2.avro"
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database_api/incremental_users"
    val bronzeDF1 = spark.read.format("avro").load(expectedBronzePath1)
    val bronzeDF2 = spark.read.format("avro").load(expectedBronzePath2)
    val silverDF = spark.sql("select * from database_api.incremental_users")

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head
    val activityStatistics2 = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath1, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath1), Some(expectedSilverPath), Some(2), Some(0), Some(2),
      Some(0),
      Some(ActivitySilverPersistence(Some("database_api"), Some("incremental_users"), None, Some(0L), None, None,
        None)))

    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, avroFilePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(3),
      Some(0), Some(ActivitySilverPersistence(Some("database_api"), Some("incremental_users"), Some(0L), Some(1L), None, None,
        None)))

    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-api-incremental-users", IngestionTypes.Incremental, ValidationTypes.FailFast, "UserName/Timestamp_yyyy/Timestamp_mm/Timestamp_dd")
    assertActivityDataset(activityStatistics2, "dataset-api-incremental-users", IngestionTypes.Incremental, ValidationTypes.FailFast, "UserName/Timestamp_yyyy/Timestamp_mm/Timestamp_dd")

    assert(Files.exists(Paths.get(expectedBronzePath1)))
    assert(Files.exists(Paths.get(expectedBronzePath2)))
    assert(bronzeDF1.count() == 2)
    assert(bronzeDF2.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 5)
    assertInfoColumns(silverDF)
    assert(Files.notExists(Paths.get(avroFilePath1)))
    assert(Files.notExists(Paths.get(avroFilePath2)))

    assert(Files.exists(Paths.get(expectedSilverPath + "/UserName=Pedro/Timestamp_yyyy=2022/Timestamp_mm=1/Timestamp_dd=1")))
    assert(Files.exists(Paths.get(expectedSilverPath + "/UserName=Juan/Timestamp_yyyy=2022/Timestamp_mm=1/Timestamp_dd=1")))
    assert(Files.exists(Paths.get(expectedSilverPath + "/UserName=Cristian/Timestamp_yyyy=2022/Timestamp_mm=1/Timestamp_dd=2")))
    assert(Files.exists(Paths.get(expectedSilverPath + "/UserName=Maria/Timestamp_yyyy=2022/Timestamp_mm=1/Timestamp_dd=2")))

    val schemaEvoWithPartition = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
      ("Timestamp_yyyy", IntegerType, true),
      ("Timestamp_mm", IntegerType, true),
      ("Timestamp_dd", IntegerType, true),
      ("Gender", StringType, true)
    )

    val silverTable = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", 2022, 1, 1, null),
        ("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00", 2022, 1, 1, null),
        ("Cristian", 15L, "Cristian", "Morillas", "2022-01-02 01:00:00", 2022, 1, 2, "M"),
        ("Maria", 21L, "Maria", "", "2022-01-02 01:00:00", 2022, 1, 2, "F"),
        ("Maria", 17L, "Maria", "Ramirez", "2022-01-02 01:00:00", 2022, 1, 2, "F")
      ),
      schemaEvoWithPartition
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
  }

  test("IngestApiActivitySpec# ingestion last changes all OK") {
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-last-changes-users")
    val avroFilePath1 = this.testResourcesBasePath + "landing/streaming/tmp/20220101_dataset-api-last-changes-users.bz2.avro"
    val avroFilePath2 = this.testResourcesBasePath + "landing/streaming/tmp/20220102_dataset-api-last-changes-users.bz2.avro"

    val schema = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Gender", StringType, true),
      ("Timestamp", StringType, true),
      ("_corrupt_record", StringType, true)
    )
    val schemaNoCorruptRecord = schema.filter(!_._1.equals("_corrupt_record"))

    val dataframe1 = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "F", "2022-01-01 00:00:00", null),
        ("Juan", 8L, "Juan", "Clavijo", "M", "2022-01-01 00:00:00", null)
      ),
      schema
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

    val dataframe2 = spark.createDF(
      List(
        ("Cristian", 15L, "Cristian", "Morillas", "M", "2022-01-02 01:00:00", null),
        ("Juan", 15L, "Juan", "Perez", "F", "2022-01-02 01:01:01", null)
      ),
      schema
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

    dataframe1
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath1)

    dataframe2
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath2)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe1, avroFilePath1))
    ingestApiCallActivity.execute(dataset.get,adfRunId)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe2, avroFilePath2))
    ingestApiCallActivity.execute(dataset.get,adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-api/dataset-api-last-changes-users/")

    val expectedBronzePath1 = this.indationProperties.datalake.basePath +
      dirs + "20220101_dataset-api-last-changes-users.bz2.avro"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      dirs + "20220102_dataset-api-last-changes-users.bz2.avro"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database_api/last_changes_users"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database_api/last_changes_users" + historicalSuffix

    val bronzeDF1 = spark.read.format("avro").load(expectedBronzePath1)
    val bronzeDF2 = spark.read.format("avro").load(expectedBronzePath2)
    val silverDF = spark.sql("select * from database_api.last_changes_users")
    val silverDFHistorical = spark.sql("select * from database_api.last_changes_users" + historicalSuffix)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head
    val activityStatistics2 = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath1, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath1), Some(expectedSilverPath), Some(2), Some(0), Some(2),
      Some(0),
      Some(ActivitySilverPersistence(Some("database_api"), Some("last_changes_users"), None, Some(0L), Some("last_changes_users" + historicalSuffix), None,
        Some(0L))))
    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityStatistics(activityStatistics2, avroFilePath2, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(2), Some(0), Some(2),
      Some(0),
      Some(ActivitySilverPersistence(Some("database_api"), Some("last_changes_users"), None, Some(0L), Some("last_changes_users" + historicalSuffix), Some(0L),
        Some(1L))))
    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-api-last-changes-users", IngestionTypes.LastChanges, ValidationTypes.FailFast)
    assertActivityDataset(activityStatistics2, "dataset-api-last-changes-users", IngestionTypes.LastChanges, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(avroFilePath1)))
    assert(Files.notExists(Paths.get(avroFilePath2)))

    assert(Files.exists(Paths.get(expectedBronzePath1)))
    assert(bronzeDF1.count() == 2)
    assert(Files.exists(Paths.get(expectedBronzePath2)))
    assert(bronzeDF2.count() == 2)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
    assert(silverDFHistorical.count() == 4)
    assertInfoColumns(silverDF)
    assertInfoColumns(silverDFHistorical)

    val silverTable = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "F", "2022-01-01 00:00:00"),
        ("Cristian", 15L, "Cristian", "Morillas", "M", "2022-01-02 01:00:00"),
        ("Juan", 15L, "Juan", "Perez", "F", "2022-01-02 01:01:01")
      ),
      schemaNoCorruptRecord
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "F", "2022-01-01 00:00:00"),
        ("Juan", 8L, "Juan", "Clavijo", "M", "2022-01-01 00:00:00"),
        ("Cristian", 15L, "Cristian", "Morillas", "M", "2022-01-02 01:00:00"),
        ("Juan", 15L, "Juan", "Perez", "F", "2022-01-02 01:01:01")
      ),
      schemaNoCorruptRecord
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
      silverHistoricalTable, orderedComparison = false)
  }

  test("IngestApiActivitySpec# ingestion last changes invalid KO") {
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-last-changes-users")
    val avroFilePath1 = this.testResourcesBasePath + "landing/streaming/tmp/20220103_dataset-api-last-changes-users.bz2.avro"
    val avroFilePath2 = this.testResourcesBasePath + "landing/streaming/tmp/20220104_dataset-api-last-changes-users.bz2.avro"

    val schema1 = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
      ("Gender", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val schema2 = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
      ("_corrupt_record", StringType, true)
    )

    val schemaNoCorruptRecord = schema1.filter(!_._1.equals("_corrupt_record"))

    val dataframe1 = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", "M", null),
        ("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00", "F", null)
      ),
      schema1
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

    val dataframe2 = spark.createDF(
      List(
        ("Cristian", 15L, "Cristian", "Morillas", "2022-01-02 01:00:00", null),
        ("Juan", 15L, "Juan", "Perez", "2022-01-02 01:01:01", null)
      ),
      schema2
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

    dataframe1
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath1)

    dataframe2
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath2)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe1, avroFilePath1))
    ingestApiCallActivity.execute(dataset.get,adfRunId)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe2, avroFilePath2))
    ingestApiCallActivity.execute(dataset.get,adfRunId)

    val dirsBronze = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-api/dataset-api-last-changes-users/")
    val dirsInvalid = FileNameUtils.destinationSubdirsPath("", dataset.get, "landing/invalid/source-api/dataset-api-last-changes-users/")


    val expectedBronzePath1 = this.indationProperties.datalake.basePath +
      dirsBronze + "20220103_dataset-api-last-changes-users.bz2.avro"
    val expectedInvalidPath2 = this.indationProperties.datalake.basePath +
      dirsInvalid + "20220104_dataset-api-last-changes-users.bz2.avro"

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database_api/last_changes_users"
    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database_api/last_changes_users" + historicalSuffix

    val bronzeDF1 = spark.read.format("avro").load(expectedBronzePath1)
    val silverDF = spark.sql("select * from database_api.last_changes_users")
    val silverDFHistorical = spark.sql("select * from database_api.last_changes_users" + historicalSuffix)

    val stats = this.ingestionStats()
    val activityStatistics = stats.tail.head
    val activityStatistics2 = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath1, ActivityResults.Ingested_Without_Errors, None,
      None, None, None, Some(expectedBronzePath1), Some(expectedSilverPath), Some(2), Some(0), Some(2),
      Some(0),
      Some(ActivitySilverPersistence(Some("database_api"), Some("last_changes_users"), None, Some(0L), Some("last_changes_users" + historicalSuffix), None,
        Some(0L))))
    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
    assertActivityStatistics(activityStatistics2, avroFilePath2, ActivityResults.RejectedInvalid,None,
      None, None, Some(expectedInvalidPath2), None,
      None, Some(2), Some(0), Some(2), Some(0), None)

    assertActivityDataset(activityStatistics, "dataset-api-last-changes-users", IngestionTypes.LastChanges, ValidationTypes.FailFast)
    assertActivityDataset(activityStatistics2, "dataset-api-last-changes-users", IngestionTypes.LastChanges, ValidationTypes.FailFast)

    assert(Files.notExists(Paths.get(avroFilePath1)))
    assert(Files.notExists(Paths.get(avroFilePath2)))

    assert(Files.exists(Paths.get(expectedBronzePath1)))
    assert(bronzeDF1.count() == 2)
    assert(Files.exists(Paths.get(expectedInvalidPath2)) )
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 2)
    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
    assert(silverDFHistorical.count() == 2)
    assertInfoColumns(silverDF)
    assertInfoColumns(silverDFHistorical)

    val silverTable = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", "M"),
        ("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00", "F")
      ),
      schemaNoCorruptRecord
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", "M"),
        ("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00", "F")
      ),
      schemaNoCorruptRecord
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
      silverHistoricalTable, orderedComparison = false)
  }

  test("IngestApiActivitySpec# ingestion full-snapshot sensitive, all OK") {
    val fs: FSCommands = FSCommandsFactory.getFSCommands(this.indationProperties.environment)
    val landingApiReader: LandingApiReader = mock[LandingApiReader]
    val ingestApiCallActivity = new IngestApiCallActivity(indationProperties, landingApiReader)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-sensitive")
    val avroFilePath = this.testResourcesBasePath + "landing/streaming/tmp/20220101_dataset-api-sensitive.bz2.avro"

    val schema = SchemaHelper.structTypeSchema(dataset.get)

    val data = List(
      Row("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00", 23, List(1L, 2L), Row("Spain", Row(435L, "Alicante", List(10,12,15)), "Elche"), List(List(1,2,3),List(3,4,5)), List(Row("test", 5), Row("test2", 10)), List(List(Row("prueba",2)))),
      Row("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00", 42, List(null), Row("Spain", Row(143L, "Madrid", List(41,11,32)), "Madrid"), List(List(6,7,8),List(9,10,11)), List(Row("test3", 15), Row("test4", 20)), List(List(Row("prueba",2)))),
      Row(null, 8L, "Pepe", "Andrés", "2022-01-01 00:00:00", 42, List(10L), Row("Spain", Row(null, "Madrid", List(41,11,32)), "Madrid"), List(List(null,7,8),List()), List(Row("test3", 15), Row("test4", 20)), List(List(Row("prueba",2))))
    )

    val rdd = spark.sparkContext.parallelize(data)
    val dataframe = spark.createDataFrame(rdd,schema=schema)
//      .withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("_corrupt_record", lit(null).cast(StringType))

    dataframe
      .write
      .format("avro")
      .option("compression", "bzip2")
      .save(avroFilePath)

    when(landingApiReader.readApi(any[Source], any[Dataset], any[Option[String]])).thenReturn((dataframe, avroFilePath))

    ingestApiCallActivity.execute(dataset.get,adfRunId)

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-api/dataset-api-sensitive/")

    val expectedBronzePathEncrypted = this.indationProperties.datalake.basePath +
      dirs + "encrypted_" + "20220101_dataset-api-sensitive.bz2.avro.zip"
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database_api/api_sensitive"
    val expectedSilverPath_sensitive = this.indationProperties.datalake.basePath + "silver/public/database_api/api_sensitive_sensitive"
    val expectedSilverPath_historical = this.indationProperties.datalake.basePath + "silver/public/database_api/api_sensitive_historical"
    val expectedSilverPath_historical_sensitive = this.indationProperties.datalake.basePath + "silver/public/database_api/api_sensitive_sensitive_historical"
    val bronzeDF = spark.read.format("avro").load(fs.uncompressZipFolder(decryptFile(expectedBronzePathEncrypted)))

    val silverDF = spark.sql("select * from database_api.api_sensitive")
    val silverDF_sensitive = spark.sql("select * from database_api.api_sensitive_sensitive")
    val silverDF_historical = spark.sql("select * from database_api.api_sensitive_historical")
    val silverDF_historical_sensitive = spark.sql("select * from database_api.api_sensitive_sensitive_historical")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, avroFilePath, ActivityResults.Ingested_Without_Errors,None,
      None, None, None, Some(expectedBronzePathEncrypted),
      Some(expectedSilverPath), Some(3), Some(0), Some(3), Some(0),
      Some(ActivitySilverPersistence(Some("database_api"),Some("api_sensitive"),None,Some(0L),
        Some("api_sensitive" + historicalSuffix),None,Some(0L))))

    assertActivityDataset(activityStatistics, "dataset-api-sensitive", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)

    assert(Files.exists(Paths.get(expectedBronzePathEncrypted)))
    assert(bronzeDF.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 3)
    assertInfoColumns(silverDF)
    assert(Files.exists(Paths.get(expectedSilverPath_sensitive)))
    assert(silverDF_sensitive.count() == 3)
    assertInfoColumns(silverDF_sensitive)
    assert(Files.exists(Paths.get(expectedSilverPath_historical)))
    assert(silverDF_historical.count() == 3)
    assert(Files.exists(Paths.get(expectedSilverPath_historical_sensitive)))
    assert(silverDF_historical_sensitive.count() == 3)
    assert(Files.notExists(Paths.get(avroFilePath)))
  }

  /*test("prueba lectura y cifrado TEST_20220101_dataset-api-sensitive.bz2.avro") {
    val fs: FSCommands = FSCommandsFactory.getFSCommands(this.indationProperties.environment)

    def copyToBronze(fileInputPath: String, ds: Dataset): String = {
      val fileName = fileInputPath.split("/").last
      val prefixTargetPath = FileNameUtils.destinationSubdirsPath(fileName,
        ds,
        indationProperties.datalake.basePath + indationProperties.datalake.bronzePath + "/" + ds.sourceName + "/" + ds.name + "/")

      val destinationPath = prefixTargetPath + fileName

      fs.renameFileIfExist(destinationPath)
      fs.cp(fileInputPath, destinationPath)

      destinationPath
    }

    def copyCompressedToBronze(fileInputPath: String, ds: Dataset): String = {
      val fileName = fileInputPath.split("/").last

      val prefixTargetPath = FileNameUtils.destinationSubdirsPath(fileName,
        ds,
        indationProperties.datalake.basePath + indationProperties.datalake.bronzePath + "/" + ds.sourceName + "/" + ds.name + "/")

      val fileExtension = fileName.split("\\.").last
      if (fileExtension == "txt" || fileExtension == "csv" || fileExtension == "json" || fileExtension == "xls" || fileExtension == "xlsx") {
        Try {
          val tmpFile = indationProperties.tmpDirectory + fileName
          fs.cp(fileInputPath, tmpFile)
          val compressedTmpFile = fs.compressGzip(tmpFile)
          val destinationPath = prefixTargetPath + compressedTmpFile.split("/").last
          fs.renameFileIfExist(destinationPath)
          fs.mv(compressedTmpFile, destinationPath)
          fs.rm(tmpFile)
          destinationPath
        } match {
          case Success(path) => path
          case Failure(e) =>
            println("Error al comprimir: " + fileInputPath)
            copyToBronze(fileInputPath, ds)
        }
      }
      else {
        /////////Nuevo para prueba comprimir carpeta - Inicio/////////
        //TODO: Comprobar si se trata de algo sensible, y si es una carpeta o un fichero para tratarlos distinto
        val encryptRawFile = this.indationProperties.security.encryption.encryptRawFile.getOrElse(false)
        val sensitiveColumns = SchemaHelper.sensitiveColumns(ds)

        val file = new File(fileInputPath)

        //Si es una carpeta y se va a tener que cifrar después, vamos a comprimirla en ZIP porque no podemos cifrar carpetas
        val finalFileInputPath =
        if (encryptRawFile && sensitiveColumns.nonEmpty && file.isDirectory) {
          fs.compressZipFolder(fileInputPath)
        } else {
          fileInputPath
        }
        /////////Nuevo para prueba comprimir carpeta - Fin/////////
        copyToBronze(finalFileInputPath, ds)
      }
    }

    def copyCompressedToBronzeV2(fileInputPath: String, ds: Dataset, encryptBronze: Boolean = false): String = {
      val fileName = fileInputPath.split("/").last

      val prefixTargetPath = FileNameUtils.destinationSubdirsPath(fileName,
        ds,
        indationProperties.datalake.basePath + indationProperties.datalake.bronzePath + "/" + ds.sourceName + "/" + ds.name + "/")

      val fileExtension = fileName.split("\\.").last
      if (fileExtension == "txt" || fileExtension == "csv" || fileExtension == "json" || fileExtension == "xls" || fileExtension == "xlsx") {
        Try {
          val tmpFile = indationProperties.tmpDirectory + fileName
          fs.cp(fileInputPath, tmpFile)
          val compressedTmpFile = fs.compressGzip(tmpFile)
          val destinationPath = prefixTargetPath + compressedTmpFile.split("/").last

          //Si se va a encriptar posteriormente en bronze, el nombre del fichero tras encriptar contiene el prefijo "encrypted_",
          //por lo que renombraremos el fichero que ya existe y contenga ese prefijo.
          if (encryptBronze) {
            val destinationEncryptedPath = prefixTargetPath + "encrypted_" + compressedTmpFile.split("/").last
            fs.renameFileIfExist(destinationEncryptedPath)
          } else {
            fs.renameFileIfExist(destinationPath)
          }

          fs.mv(compressedTmpFile, destinationPath)
          fs.rm(tmpFile)
          destinationPath
        } match {
          case Success(path) => path
          case Failure(e) =>
            println("Error al comprimir: " + fileInputPath, Some(e))
            copyToBronze(fileInputPath, ds)
        }
      }
      else {
//        val file = new File(fileInputPath)

        //TODO: Hacer funcion isDirectory para cada entorno en FSCommands
        if (encryptBronze) {
          if (!fs.isDirectory(fileInputPath)) {
            //Como se va a encriptar posteriormente en bronze, el nombre del fichero tras encriptar contiene el prefijo "encrypted_",
            //por lo que renombraremos el fichero que ya existe y contenga ese prefijo.
            val destinationEncryptedPath = prefixTargetPath + "encrypted_" + fileName
            fs.renameFileIfExist(destinationEncryptedPath)

            copyToBronze(fileInputPath, ds)
          } else {
            Try {
              val tmpFile = indationProperties.tmpDirectory + fileName

              fs.cp(fileInputPath, tmpFile)
              val compressedTmpFile = fs.compressZipFolder(tmpFile)
              val destinationPath = prefixTargetPath + compressedTmpFile.split("/").last

              //Como se va a encriptar posteriormente en bronze, el nombre del fichero tras encriptar contiene el prefijo "encrypted_",
              //por lo que renombraremos el fichero que ya existe y contenga ese prefijo.
              val destinationEncryptedPath = prefixTargetPath + "encrypted_" + compressedTmpFile.split("/").last
              fs.renameFileIfExist(destinationEncryptedPath)

              fs.mv(compressedTmpFile, destinationPath)
              fs.rm(tmpFile)
              destinationPath
            } match {
              case Success(path) => path
              case Failure(e) =>
                println("Error al comprimir: " + fileInputPath, Some(e))
                copyToBronze(fileInputPath, ds)
            }
          }
        } else {
          copyToBronze(fileInputPath, ds)
        }
      }
    }

    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-api-sensitive")
    val avroFilePath = this.testResourcesBasePath + "landing/streaming/tmp/TEST_20220101_dataset-api-sensitive.bz2.avro"
    /*val avroFilePathZipped = this.testResourcesBasePath + "landing/streaming/tmp/20220101_dataset-api-sensitive.bz2.TEST_20220101_dataset-api-sensitive.bz2.avro.zip"

    val dirs = FileNameUtils.destinationSubdirsPath("", dataset.get, "bronze/source-api/dataset-api-sensitive/")
    val expectedBronzePath = this.indationProperties.datalake.basePath +
      dirs + "20220101_dataset-api-sensitive.bz2.TEST_20220101_dataset-api-sensitive.bz2.avro" + "/" + "part-00005-ec0f86f3-be2a-4d22-9b20-c21cb1567569-c000.TEST_20220101_dataset-api-sensitive.bz2.avro"
    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
      dirs + "20220101_dataset-api-sensitive.bz2.TEST_20220101_dataset-api-sensitive.bz2.avro"

    val expectedBronzePathZipped = this.indationProperties.datalake.basePath +
      dirs + "20220101_dataset-api-sensitive.bz2.TEST_20220101_dataset-api-sensitive.bz2.avro.zip"*/

    val compressedBronzePath = copyCompressedToBronzeV2(avroFilePath , dataset.get, true)
    val encryptedBronzeAbsolutePath = encryptFile(compressedBronzePath)
    val decryptedBronzeAbsolutePath = decryptFile(encryptedBronzeAbsolutePath)
    val uncompressedBronzePath = fs.uncompressZipFolder(decryptedBronzeAbsolutePath)

    val dfInicial = spark.read
      .format("avro")
      .load(avroFilePath)
    val dfFinal = spark.read
      .format("avro")
      .load(uncompressedBronzePath)

    dfInicial.show(20,false)
    dfFinal.show(20,false)

    //Encripta y desencripta bien un zip
//    val encryptedBronzeAbsolutePath = encryptFile(avroFilePathZipped)
//    val decryptedBronzeAbsolutePath = decryptFile(encryptedBronzeAbsolutePath)


    /*val df = spark
      .read
      //.schema(schemaFromJson)
      .format("TEST_20220101_dataset-api-sensitive.bz2.avro")
      .load(avroFilePath)

    val dfBronze = spark
      .read
      .format("TEST_20220101_dataset-api-sensitive.bz2.avro")
      .load(expectedBronzePath)

    df.show(100,false)
    dfBronze.show(100,false)*/
  }

  test("tabla cmp sensible"){
    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)

    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-actividad-alias-pseudoanonimizacion")

    ingestJdbcTableActivity.execute(dataset.get, adfRunId)

    val df = spark.sql("select * from cmp_encrypted.ActividadAliasPseudo")
    val df_sensitive = spark.sql("select * from cmp_encrypted.ActividadAliasPseudo_sensitive")

    df.show(50,false)
    df_sensitive.show(50,false)

//    val path= this.testResourcesBasePath + "landing/streaming/tmp/encrypted_dbo_Actividad_20230307_114238.csv.bz2"
//    val decryptedBronzeAbsolutePath = decryptFile(path)

    ingestJdbcTableActivity.execute(dataset.get, adfRunId)

    println("hola")
  }*/

}
