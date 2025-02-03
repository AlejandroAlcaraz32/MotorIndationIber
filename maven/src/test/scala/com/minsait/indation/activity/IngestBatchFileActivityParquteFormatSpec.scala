//// TODO: Hay que averiguar por qué estas validaciones funcionan bien por separado, pero no es compatible ejecutar BatchFileActivity de Parquet y Delta en el mismo bloque
//
//package com.minsait.indation.activity
//
//import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
//import com.github.mrpowers.spark.fast.tests.DatasetComparer
//import com.minsait.indation.BuildInfo
//import com.minsait.indation.activity.exceptions.IngestionException
//import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
//import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
//import com.minsait.indation.configuration.models.EnvironmentTypes.Local
//import com.minsait.indation.configuration.models._
//import com.minsait.indation.metadata.MetadataFilesManager
//import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, ValidationTypes}
//import com.minsait.indation.spark.{SparkSessionFactory, SparkSessionWrapper}
//import org.apache.commons.io.FileUtils
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType, TimestampType}
//import org.mockito.MockitoSugar
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
//import spray.json.enrichString
//
//import java.io.File
//import java.nio.file.{Files, Paths}
//import java.sql.Timestamp
//
//class IngestBatchFileActivityParquteFormatSpec extends AnyFunSuite with MockitoSugar  with BeforeAndAfterAll with BeforeAndAfterEach
//  with DatasetComparer with SparkSessionWrapper {
//
//  private val DirectorySeparator: String = "/"
//
//  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
//    .toString
//    .replace("\\",DirectorySeparator) + DirectorySeparator
//
//  private val tmpDirectory = testResourcesBasePath + "tmp/"
//
//  private val indationProperties: IndationProperties = IndationProperties(
//    MetadataProperties("",testResourcesBasePath,"","")
//    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
//    , LandingProperties("AccountName1", testResourcesBasePath + "landing2/" , "", "pending", "unknown"
//      , "invalid", "corrupted", "schema-mismatch", "streaming")
//    , DatalakeProperties("",testResourcesBasePath,"", Some(DatalakeOutputTypes.Parquet))
//    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
//    , Local,
//    tmpDirectory
//  )
//
//  private val dataLakeStorageDate = "datalake_load_date"
//  private val dataLakeStorageDay = "datalake_load_day"
//  private val dataLakeIngestionUuid = "datalake_ingestion_uuid"
//  private val yyyy = "yyyy"
//  private val mm = "mm"
//  private val dd = "dd"
//  private val historicalSuffix = "_historical"
//  private val adfRunId = "adf-uuid"
//
//  override def beforeAll() {
//    super.beforeAll()
//    SparkSessionFactory.configSparkSession(indationProperties)
//  }
//
//  override def afterEach(): Unit = {
//    FileUtils.deleteDirectory(Paths.get(testResourcesBasePath + "silver").toFile)
//  }
//    override def beforeEach(): Unit = {
//      super.beforeEach()
//      // clean full metastore
//      spark.catalog.listDatabases.collect().foreach(db => {
//        if (db.name != "default") {
//          spark.sql("DROP DATABASE IF EXISTS " + db.name + " CASCADE")
//        }
//      })
//      // clean full silver
//      FileUtils.deleteDirectory(Paths.get(testResourcesBasePath + "silver").toFile)
//
//    }
//  private def ingestionStats(): List[ActivityStatistics] = {
//
//    spark.sql("select * from applications.indation order by " + dataLakeStorageDate + " desc")
//      .toJSON
//      .collect
//      .toList
//      .map(_.parseJson.convertTo[ActivityStatistics])
//  }
//
//  private def assertActivityStatistics(activityStatistics: ActivityStatistics,
//                                       filePath: String,
//                                       result: ActivityResults.ActivityResult,
//                                       unknownAbsolutePath: Option[String],
//                                       corruptedAbsolutePath: Option[String],
//                                       schemaMismatchAbsolutePath: Option[String],
//                                       invalidAbsolutePath: Option[String],
//                                       bronzeAbsolutePath: Option[String],
//                                       silverAbsolutePath: Option[String],
//                                       bronzeValidRows: Option[Long],
//                                       bronzeInvalidRows: Option[Long],
//                                       silverValidRows: Option[Long],
//                                       silverInvalidRows: Option[Long],
//                                       silverPersistence: Option[ActivitySilverPersistence]
//                              ): Unit = {
//    assertResult(ActivityTriggerTypes.Adf)(activityStatistics.trigger.typ)
//    assertResult(adfRunId)(activityStatistics.trigger.id)
//    assertResult(BuildInfo.name)(activityStatistics.engine.get.name)
//    assertResult(BuildInfo.version)(activityStatistics.engine.get.version)
//    assertResult(this.indationProperties.landing.basePath + filePath)(activityStatistics.origin)
//    assertResult(result)(activityStatistics.result)
//    assertResult(true)(activityStatistics.execution.start.compareTo(new Timestamp(System.currentTimeMillis)) < 0)
//    assertResult(true)(activityStatistics.execution.end.compareTo(new Timestamp(System.currentTimeMillis)) < 0)
//    assertResult(true)(activityStatistics.execution.duration > 0)
//    assertResult(unknownAbsolutePath.getOrElse(None))(activityStatistics.output_paths.unknown.getOrElse(None))
//    assertResult(corruptedAbsolutePath.getOrElse(None))(activityStatistics.output_paths.corrupted.getOrElse(None))
//    assertResult(schemaMismatchAbsolutePath.getOrElse(None))(activityStatistics.output_paths.schema_mismatch.getOrElse(None))
//    assertResult(invalidAbsolutePath.getOrElse(None))(activityStatistics.output_paths.invalid.getOrElse(None))
//    assertResult(bronzeAbsolutePath.getOrElse(None))(activityStatistics.output_paths.bronze.getOrElse(None))
//    assertResult(silverAbsolutePath.getOrElse(None))(activityStatistics.output_paths.silver_principal.getOrElse(None))
//    if(activityStatistics.rows.isDefined) {
//      assertResult(bronzeValidRows.getOrElse(None))(activityStatistics.rows.get.bronze_valid.getOrElse(None))
//      assertResult(bronzeInvalidRows.getOrElse(None))(activityStatistics.rows.get.bronze_invalid.getOrElse(None))
//      assertResult(silverInvalidRows.getOrElse(None))(activityStatistics.rows.get.silver_invalid.getOrElse(None))
//      assertResult(silverValidRows.getOrElse(None))(activityStatistics.rows.get.silver_valid.getOrElse(None))
//    } else {
//      assertResult(None)(activityStatistics.rows)
//    }
//    if(activityStatistics.silver_persistence.isDefined) {
//      assertResult(silverPersistence.get.database.getOrElse(None))(activityStatistics.silver_persistence.get.database.getOrElse(None))
//      assertResult(silverPersistence.get.principal_table.getOrElse(None))(activityStatistics.silver_persistence.get.principal_table.getOrElse(None))
//      // En PARQUET no validamos versiones, eso es propio sólo de DELTA
////      assertResult(silverPersistence.get.principal_previous_version.getOrElse(None))(activityStatistics.silver_persistence.get.principal_previous_version.getOrElse(None))
////      assertResult(silverPersistence.get.principal_current_version.getOrElse(None))(activityStatistics.silver_persistence.get.principal_current_version.getOrElse(None))
//      assertResult(silverPersistence.get.historical_table.getOrElse(None))(activityStatistics.silver_persistence.get.historical_table.getOrElse(None))
//      // En PARQUET no validamos versiones, eso es propio sólo de DELTA
////      assertResult(silverPersistence.get.historical_previous_version.getOrElse(None))(activityStatistics.silver_persistence.get.historical_previous_version.getOrElse(None))
////      assertResult(silverPersistence.get.historical_current_version.getOrElse(None))(activityStatistics.silver_persistence.get.historical_current_version.getOrElse(None))
//    } else {
//      assertResult(None)(activityStatistics.silver_persistence)
//    }
//  }
//
//  private def assertActivityDataset(activityStatistics: ActivityStatistics,
//                                       name: String,
//                                       ingestion: IngestionTypes.IngestionType,
//                                       validation: ValidationTypes.ValidationType
//                                      ): Unit = {
//    assertResult(name)(activityStatistics.dataset.get.name)
//    assertResult(DatasetTypes.File)(activityStatistics.dataset.get.typ)
//    assertResult(1)(activityStatistics.dataset.get.version)
//    assertResult(ingestion)(activityStatistics.dataset.get.ingestion_mode)
//    assertResult(validation)(activityStatistics.dataset.get.validation_mode)
//    assertResult("yyyy/mm/dd")(activityStatistics.dataset.get.partition_by)
//  }
//
//  private def assertInfoColumns(df: DataFrame): Unit = {
//    assert(df.columns.contains(dataLakeStorageDate))
//    assert(df.columns.contains(dataLakeIngestionUuid))
//    assert(df.columns.contains(yyyy))
//    assert(df.columns.contains(mm))
//    assert(df.columns.contains(dd))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# prueba hadoop"){
//    import spark.implicits._
//    spark.sql("create database if not exists prueba")
//    val newDF = Seq(
//      ("B", 2020, 1, 2),
//      ("C", 2020, 1, 2)
//    ).toDF("col1", "yyyy", "mm", "dd")
//    val path = this.indationProperties.datalake.basePath  + "/prueba/tabla"
//    newDF.write.mode("overwrite").partitionBy("col1").parquet(path)
//    spark.sql(s"create table if not exists prueba.tabla2 using PARQUET location '$path'")
//    spark.sql("MSCK REPAIR TABLE prueba.tabla2")
//    assert(spark.sql("select * from prueba.tabla2").count()==2)
//    spark.sql(s"drop table if exists prueba.tabla2")
//    FileUtils.cleanDirectory(new File(path))
//  }
//
//  test("IngestBatchFileActivityParquetOutput#dataset compressGzip should compress file in bronze ") {
//
//    val statisticsCount = {
//      if (spark.catalog.databaseExists("applications") && spark.catalog.tableExists("applications.indation"))
//        this.ingestionStats().length
//      else
//        0
//    }
//
//    val filePath = "pending/source1/20200105_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//    val expectedTextPath = this.indationProperties.datalake.basePath  +
//      "bronze/source1/full-snapshot-worldcities/2020/01/05/20200105_worldCities.csv.gz"
//
//    assert(Files.exists(Paths.get(expectedTextPath)))
//
//    val stats2 = this.ingestionStats()
//
//    assert(statisticsCount<stats2.length)
//
//  }
//
//  test("IngestBatchFileActivityParquetOutput#dataset unknown") {
//    val filePath = "pending/source1/20200101_unknown_file.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedUnknownPath = this.indationProperties.landing.basePath  + "unknown/source1/20200101_unknown_file.csv"
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedUnknown, Some(expectedUnknownPath),
//      None, None, None, None,
//      None, None, None, None, None, None)
//
//    assert(Files.exists(Paths.get(expectedUnknownPath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput#dataset file not found") {
//    val filePath = "pending/source1/30000102_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    //TODO Cambiar este try si se decide no propagar todos los errores
//    try {
//      ingestBatchActivity.execute(filePath, adfRunId)
//    }
//    catch {
//      case ex: IngestionException => assert(ex.getMessage=="Error executing file ingest with filename: pending/source1/30000102_worldCities.csv")
//    }
//  }
//
//  test("IngestBatchFileActivityParquetOutput#dataset file corrupt") {
//    val filePath = "pending/source1/20200101_corrupt.snappy.parquet"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    val expectedCorruptPath = this.indationProperties.landing.basePath  + "corrupted/source1/dataset8/2020/01/01/20200101_corrupt.snappy.parquet"
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedCorrupted, None,
//      Some(expectedCorruptPath), None, None, None,
//      None, None, None, None, None, None)
//
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "dataset8", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion schema-mismatch fail-fast") {
//    val filePath = "pending/source1/20200102_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/full-snapshot-worldcities/2020/01/02/20200102_worldCities.csv"
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch,None,
//      None, Some(expectedSchemaMismatchPath), None, None,
//      None, Some(1), Some(1), None, None, None)
//
//    assertActivityDataset(activityStatistics, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)) )
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion schema-mismatch permissive percentage ko") {
//    val filePath = "pending/source1/20200101_permissive_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/permissive-worldcities/2020/01/01/20200101_permissive_worldCities.csv"
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedSchemaMismatch,None,
//      None, Some(expectedSchemaMismatchPath), None, None,
//      None, Some(4), Some(5), None, None, None)
//
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)) )
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion schema-mismatch permissive percentage ok") {
//    val filePath = "pending/source1/20200102_permissive_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/permissive-worldcities/2020/01/02/20200102_permissive_worldCities.csv.bz2"
//
//    val notExpectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/permissive-worldcities/2020/01/02/20200102_permissive_worldCities.csv"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/permissive-worldcities/2020/01/02/20200102_permissive_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/permissive_worldcities"
//
//    val shemaMismatchDF = spark.read.csv(expectedSchemaMismatchPath)
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.permissive_worldcities")
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors,None,
//      None, Some(expectedSchemaMismatchPath), None, Some(expectedBronzePath),
//      Some(expectedSilverPath), Some(8), Some(1), Some(8), Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("permissive_worldcities"),None,Some(0L),Some("permissive_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
//    assert(shemaMismatchDF.count() == 1)
//    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 9)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 8)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion invalid fail-fast") {
//    val filePath = "pending/source1/20200103_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/full-snapshot-worldcities/2020/01/03/20200103_worldCities.csv"
//
//    val notExpectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/full-snapshot-worldcities/2020/01/03/20200103_worldCities.csv.bz2"
//
//    val notExpectedSilverPath = this.indationProperties.datalake.basePath +
//      "silver/public/database1/full_snapshot_worldcities"
//
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedInvalid,None,
//      None, None, Some(expectedInvalidPath), None,
//      None, Some(10), Some(0), Some(9), Some(1), None)
//
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//
//    assert(Files.exists(Paths.get(expectedInvalidPath)))
//    assert(Files.notExists(Paths.get(notExpectedBronzePath)))
//    assert(Files.notExists(Paths.get(notExpectedSilverPath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion invalid permissive percentage ko") {
//    val filePath = "pending/source1/20200103_permissive_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/permissive-worldcities/2020/01/03/20200103_permissive_worldCities.csv"
//
//    val notExpectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/permissive-worldcities/2020/01/03/20200103_permissive_worldCities.csv.bz2"
//
//    val notExpectedSilverPath = this.indationProperties.datalake.basePath +
//      "silver/public/database1/permissive_worldcities"
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.RejectedInvalid,None,
//      None, None, Some(expectedInvalidPath), None,
//      None, Some(10), Some(0), Some(6), Some(4), None)
//
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//
//    assert(Files.exists(Paths.get(expectedInvalidPath)))
//    assert(Files.notExists(Paths.get(notExpectedBronzePath)))
//    assert(Files.notExists(Paths.get(notExpectedSilverPath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion invalid permissive percentage ok") {
//    val filePath = "pending/source1/20200104_permissive_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/permissive-worldcities/2020/01/04/20200104_permissive_worldCities.csv.bz2"
//
//    val expectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/permissive-worldcities/2020/01/04/20200104_permissive_worldCities.csv.bz2"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/permissive-worldcities/2020/01/04/20200104_permissive_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/permissive_worldcities"
//
//    val invalidDF = spark.read.csv(expectedInvalidPath)
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.permissive_worldcities")
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors,None,
//      None, None, Some(expectedInvalidPath), Some(expectedBronzePath),
//      Some(expectedSilverPath), Some(20), Some(0), Some(18), Some(2),
//      Some(ActivitySilverPersistence(Some("database1"),Some("permissive_worldcities"),None,Some(0L),Some("permissive_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//
//    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
//    assert(Files.exists(Paths.get(expectedInvalidPath)))
//    assert(invalidDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 20)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 18)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion schema-mismatch invalid permissive percentage ok") {
//    val filePath = "pending/source1/20200105_permissive_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/permissive-worldcities/2020/01/05/20200105_permissive_worldCities.csv.bz2"
//
//    val expectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/permissive-worldcities/2020/01/05/20200105_permissive_worldCities.csv.bz2"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/permissive-worldcities/2020/01/05/20200105_permissive_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/permissive_worldcities"
//
//    val schemaMismatchDF = spark.read.csv(expectedSchemaMismatchPath)
//    val invalidDF = spark.read.csv(expectedInvalidPath)
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.permissive_worldcities")
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors,None,
//      None, Some(expectedSchemaMismatchPath), Some(expectedInvalidPath), Some(expectedBronzePath),
//      Some(expectedSilverPath), Some(19), Some(1), Some(17), Some(2),
//      Some(ActivitySilverPersistence(Some("database1"),Some("permissive_worldcities"),None,Some(0L),Some("permissive_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
//    assert(schemaMismatchDF.count() == 1)
//    assert(Files.exists(Paths.get(expectedInvalidPath)))
//    assert(invalidDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 20)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 17)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion. All OK") {
//    val filePath = "pending/source1/20200101_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/full-snapshot-worldcities/2020/01/01/20200101_worldCities.csv.bz2"
//
//    val notExpectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/full-snapshot-worldcities/2020/01/01/20200101_worldCities.csv"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//                                "bronze/source1/full-snapshot-worldcities/2020/01/01/20200101_worldCities.csv.gz"
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities"
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.full_snapshot_worldcities")
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath),
//      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("full_snapshot_worldcities"),None,Some(0L),Some("full_snapshot_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//
//    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
//    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 23018)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 23018)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion incremental permissive ok 2 files") {
//    val filePath = "pending/source1/20200101_incremental_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val filePath2 = "pending/source1/20200102_incremental_worldCities.csv"
//    ingestBatchActivity.execute(filePath2, adfRunId)
//
//    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/incremental-worldcities/2020/01/01/20200101_incremental_worldCities.csv.bz2"
//    val expectedSchemaMismatchPath2 = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/incremental-worldcities/2020/01/02/20200102_incremental_worldCities.csv.bz2"
//
//    val expectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/incremental-worldcities/2020/01/01/20200101_incremental_worldCities.csv.bz2"
//    val expectedInvalidPath2 = this.indationProperties.landing.basePath  +
//      "invalid/source1/incremental-worldcities/2020/01/02/20200102_incremental_worldCities.csv.bz2"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/incremental-worldcities/2020/01/01/20200101_incremental_worldCities.csv.gz"
//    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
//      "bronze/source1/incremental-worldcities/2020/01/02/20200102_incremental_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/incremental_worldcities"
//
//    val schemaMismatchDF = spark.read.csv(expectedSchemaMismatchPath)
//    val invalidDF = spark.read.csv(expectedInvalidPath)
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val schemaMismatchDF2 = spark.read.csv(expectedSchemaMismatchPath2)
//    val invalidDF2 = spark.read.csv(expectedInvalidPath2)
//    val bronzeDF2 = spark.read.option("header","true").csv(expectedBronzePath2)
//    val silverDF = spark.sql("select * from database1.incremental_worldcities")
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//    val activityStatistics2 = stats.tail.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors,None,
//      None, Some(expectedSchemaMismatchPath), Some(expectedInvalidPath), Some(expectedBronzePath),
//      Some(expectedSilverPath), Some(19), Some(1), Some(17), Some(2),
//      Some(ActivitySilverPersistence(Some("database1"),Some("incremental_worldcities"),None,Some(0L),None,None,
//        None)))
//
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_With_Errors,None,
//      None, Some(expectedSchemaMismatchPath2), Some(expectedInvalidPath2), Some(expectedBronzePath2),
//      Some(expectedSilverPath), Some(18), Some(1), Some(16), Some(2),
//      Some(ActivitySilverPersistence(Some("database1"),Some("incremental_worldcities"),Some(0L),Some(1L),None,None,
//        None)))
//
//    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "incremental-worldcities", IngestionTypes.Incremental, ValidationTypes.Permissive)
//    assertActivityDataset(activityStatistics2, "incremental-worldcities", IngestionTypes.Incremental, ValidationTypes.Permissive)
//
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
//    assert(schemaMismatchDF.count() == 1)
//    assert(Files.exists(Paths.get(expectedInvalidPath)))
//    assert(invalidDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 20)
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath2)))
//    assert(schemaMismatchDF2.count() == 1)
//    assert(Files.exists(Paths.get(expectedInvalidPath2)))
//    assert(invalidDF2.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath2)))
//    assert(bronzeDF2.count() == 19)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 33)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion full permissive ok 2 files") {
//    val filePath = "pending/source1/20200106_permissive_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val filePath2 = "pending/source1/20200107_permissive_worldCities.csv"
//    ingestBatchActivity.execute(filePath2, adfRunId)
//
//    val expectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/permissive-worldcities/2020/01/06/20200106_permissive_worldCities.csv.bz2"
//    val expectedSchemaMismatchPath2 = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/permissive-worldcities/2020/01/07/20200107_permissive_worldCities.csv.bz2"
//
//    val expectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/permissive-worldcities/2020/01/06/20200106_permissive_worldCities.csv.bz2"
//    val expectedInvalidPath2 = this.indationProperties.landing.basePath  +
//      "invalid/source1/permissive-worldcities/2020/01/07/20200107_permissive_worldCities.csv.bz2"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/permissive-worldcities/2020/01/06/20200106_permissive_worldCities.csv.gz"
//    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
//      "bronze/source1/permissive-worldcities/2020/01/07/20200107_permissive_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/permissive_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/permissive_worldcities" + historicalSuffix
//
//    val schemaMismatchDF = spark.read.csv(expectedSchemaMismatchPath)
//    val invalidDF = spark.read.csv(expectedInvalidPath)
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val schemaMismatchDF2 = spark.read.csv(expectedSchemaMismatchPath2)
//    val invalidDF2 = spark.read.csv(expectedInvalidPath2)
//    val bronzeDF2 = spark.read.option("header","true").csv(expectedBronzePath2)
//    val silverDF = spark.sql("select * from database1.permissive_worldcities")
//    val silverDFHistorical = spark.sql("select * from database1.permissive_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//    val activityStatistics2 = stats.tail.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_With_Errors,None,
//      None, Some(expectedSchemaMismatchPath), Some(expectedInvalidPath), Some(expectedBronzePath), Some(expectedSilverPath), Some(19), Some(1), Some(17),
//      Some(2),
//      Some(ActivitySilverPersistence(Some("database1"),Some("permissive_worldcities"),None,Some(0L),Some("permissive_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_With_Errors,None,
//      None, Some(expectedSchemaMismatchPath2), Some(expectedInvalidPath2), Some(expectedBronzePath2), Some(expectedSilverPath), Some(18), Some(1), Some(16),
//      Some(2),
//      Some(ActivitySilverPersistence(Some("database1"),Some("permissive_worldcities"),Some(0L),Some(1L),Some("permissive_worldcities_historical"),Some(0L),
//        Some(1L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//    assertActivityDataset(activityStatistics2, "permissive-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.Permissive)
//
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath)))
//    assert(schemaMismatchDF.count() == 1)
//    assert(Files.exists(Paths.get(expectedInvalidPath)))
//    assert(invalidDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 20)
//    assert(Files.exists(Paths.get(expectedSchemaMismatchPath2)))
//    assert(schemaMismatchDF2.count() == 1)
//    assert(Files.exists(Paths.get(expectedInvalidPath2)))
//    assert(invalidDF2.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath2)))
//    assert(bronzeDF2.count() == 19)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 16)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(silverDFHistorical.count() == 33)
//    assertInfoColumns(silverDF)
//    assertInfoColumns(silverDFHistorical)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion last changes fail fast ok 2 files") {
//    val filePath = "pending/source1/20200101_last_changes_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val filePath2 = "pending/source1/20200102_last_changes_worldCities.csv"
//    ingestBatchActivity.execute(filePath2, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/last-changes-worldcities/2020/01/01/20200101_last_changes_worldCities.csv.gz"
//    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
//      "bronze/source1/last-changes-worldcities/2020/01/02/20200102_last_changes_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities" + historicalSuffix
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val bronzeDF2 = spark.read.option("header","true").csv(expectedBronzePath2)
//    val silverDF = spark.sql("select * from database1.last_changes_worldcities")
//    val silverDFHistorical = spark.sql("select * from database1.last_changes_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//    val activityStatistics2 = stats.tail.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("last_changes_worldcities"),None,Some(0L),Some("last_changes_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("last_changes_worldcities"),None,Some(0L),Some("last_changes_worldcities_historical"),
//        Some(0L), Some(1L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "last-changes-worldcities", IngestionTypes.LastChanges, ValidationTypes.FailFast)
//    assertActivityDataset(activityStatistics2, "last-changes-worldcities", IngestionTypes.LastChanges, ValidationTypes.FailFast)
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 3)
//    assert(Files.exists(Paths.get(expectedBronzePath2)))
//    assert(bronzeDF2.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 4)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(silverDFHistorical.count() == 6)
//    assertInfoColumns(silverDF)
//    assertInfoColumns(silverDFHistorical)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))
//
//    val spain = "Spain"
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true),
//      ("yyyy", IntegerType, true),
//      ("mm", IntegerType, true),
//      ("dd", IntegerType, true)
//    )
//
//    val silverTable = spark.createDF(
//      List(
//        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 2),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 1),
//        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 2),
//        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 2)
//      ),
//      silverSchema
//    )
//
//    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
//
//    val silverHistoricalTable = spark.createDF(
//      List(
//        ("Alicante",spain,"Valencia",2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 1),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 1),
//        ("Ciudad Real",spain,"Castille-La Mancha",2519402L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 1),
//        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 2),
//        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 2),
//        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 2)
//      ),
//      silverSchema
//    )
//
//    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid),
//      silverHistoricalTable, orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion full snapshot schema evolution") {
//    val filePath = "pending/source1/20200104_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val filePath2 = "pending/source1/20200105_evo_worldCities.csv"
//    ingestBatchActivity.execute(filePath2, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/full-snapshot-worldcities/2020/01/04/20200104_worldCities.csv.gz"
//    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
//      "bronze/source1/full-snapshot-worldcities/2020/01/05/20200105_evo_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities" + historicalSuffix
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val bronzeDF2 = spark.read.option("header","true").csv(expectedBronzePath2)
//    val silverDF = spark.sql("select * from database1.full_snapshot_worldcities")
//    var silverDFHistorical = spark.sql("select * from database1.full_snapshot_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//    val activityStatistics2 = stats.tail.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(2), Some(0), Some(2),
//      Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("full_snapshot_worldcities"),None,Some(0L),Some("full_snapshot_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(2), Some(0), Some(2),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("full_snapshot_worldcities"),Some(0L),Some(1L),Some
//      ("full_snapshot_worldcities_historical"),Some(0L),
//        Some(1L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//    assertActivityDataset(activityStatistics2, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath2)))
//    assert(bronzeDF2.count() == 2)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(silverDFHistorical.count() == 4)
//    assertInfoColumns(silverDF)
//    assertInfoColumns(silverDFHistorical)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))
//
//    val spain = "Spain"
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true),
//      ("evo1", StringType, true),
//      ("geonameid2", LongType, true),
//      ("yyyy", StringType, true),
//      ("mm", StringType, true),
//      ("dd", StringType, true)
//    )
//
//    val silverTable = spark.createDF(
//      List(
//        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), "evo1", 1L, "2020", "01", "05"),
//        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), "evo1", 2L, "2020", "01", "05")
//      ),
//      silverSchema
//    )
//
//    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
//
//    val silverSchemaHistorical = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true),
//      ("yyyy", IntegerType, true),
//      ("mm", IntegerType, true),
//      ("dd", IntegerType, true),
//      ("evo1", StringType, true),
//      ("geonameid2", LongType, true)
//    )
//
//    var silverHistoricalTable = spark.createDF(
//      List(
//        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 4, null, null),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 4, null, null),
//        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 5,"evo1", 1L),
//        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 5,"evo1", 2L)
//      ),
//      silverSchemaHistorical
//    )
//
//    silverDFHistorical =
//      silverDFHistorical
//        .drop(dataLakeStorageDate)
//        .drop(dataLakeStorageDay)
//        .drop(dataLakeIngestionUuid)
//
//    silverDFHistorical =
//      silverDFHistorical
//        .select(silverDFHistorical.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    silverHistoricalTable =
//      silverHistoricalTable
//        .select(silverHistoricalTable.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    assertSmallDatasetEquality(
//      silverDFHistorical,
//      silverHistoricalTable,
//      orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion incremental schema evolution") {
//    val filePath = "pending/source1/20200103_incremental_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val filePath2 = "pending/source1/20200104_incremental_evo_worldCities.csv"
//    ingestBatchActivity.execute(filePath2, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/incremental-worldcities/2020/01/03/20200103_incremental_worldCities.csv.gz"
//    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
//      "bronze/source1/incremental-worldcities/2020/01/04/20200104_incremental_evo_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/incremental_worldcities"
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val bronzeDF2 = spark.read.option("header","true").csv(expectedBronzePath2)
//    var silverDF = spark.sql("select * from database1.incremental_worldcities")
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//    val activityStatistics2 = stats.tail.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(2), Some(0), Some(2),
//      Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("incremental_worldcities"),None,Some(0L),None,None,
//        None)))
//
//    assertResult(None)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(2), Some(0), Some(2),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("incremental_worldcities"),Some(0L),Some(1L),None,None,
//        None)))
//
//    assertResult(None)(activityStatistics2.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "incremental-worldcities", IngestionTypes.Incremental, ValidationTypes.Permissive)
//    assertActivityDataset(activityStatistics2, "incremental-worldcities", IngestionTypes.Incremental, ValidationTypes.Permissive)
//
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 2)
//    assert(Files.exists(Paths.get(expectedBronzePath2)))
//    assert(bronzeDF2.count() == 2)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 4)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))
//
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true),
//      ("yyyy", IntegerType, true),
//      ("mm", IntegerType, true),
//      ("dd", IntegerType, true),
//      ("evo1", StringType, true),
//      ("geonameid2", LongType, true)
//    )
//
//    var silverTable = spark.createDF(
//      List(
//        ("Alicante", "Spain", "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 3, null, null),
//        ("Cáceres", "Spain", "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 3, null, null),
//        ("Ciudad Real", "Spain", "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4,"evo1", 1L),
//        ("Tomelloso", "Spain", "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4,"evo1", 2L)
//      ),
//      silverSchema
//    )
//
//    silverDF=
//      silverDF
//        .drop(dataLakeStorageDate)
//        .drop(dataLakeStorageDay)
//        .drop(dataLakeIngestionUuid)
//
//    silverDF =
//      silverDF
//        .select(silverDF.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    silverTable =
//      silverTable
//        .select(silverTable.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    assertSmallDatasetEquality(silverDF, silverTable, orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv ingestion last changes schema evolution") {
//    val filePath = "pending/source1/20200103_last_changes_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val filePath2 = "pending/source1/20200104_last_changes_evo_worldCities.csv"
//    ingestBatchActivity.execute(filePath2, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/last-changes-worldcities/2020/01/03/20200103_last_changes_worldCities.csv.gz"
//    val expectedBronzePath2 = this.indationProperties.datalake.basePath +
//      "bronze/source1/last-changes-worldcities/2020/01/04/20200104_last_changes_evo_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/last_changes_worldcities" + historicalSuffix
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val bronzeDF2 = spark.read.option("header","true").csv(expectedBronzePath2)
//    var silverDF = spark.sql("select * from database1.last_changes_worldcities")
//    var silverDFHistorical = spark.sql("select * from database1.last_changes_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//    val activityStatistics2 = stats.tail.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("last_changes_worldcities"),None,Some(0L),Some
//      ("last_changes_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityStatistics(activityStatistics2, filePath2, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath2), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("last_changes_worldcities"),Some(0L),Some(1L),Some
//      ("last_changes_worldcities_historical"),Some(0L),
//        Some(1L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics2.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "last-changes-worldcities", IngestionTypes.LastChanges, ValidationTypes.FailFast)
//    assertActivityDataset(activityStatistics2, "last-changes-worldcities", IngestionTypes.LastChanges, ValidationTypes.FailFast)
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 3)
//    assert(Files.exists(Paths.get(expectedBronzePath2)))
//    assert(bronzeDF2.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 4)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(silverDFHistorical.count() == 6)
//    assertInfoColumns(silverDF)
//    assertInfoColumns(silverDFHistorical)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))
//
//    val spain = "Spain"
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true),
//      ("yyyy", IntegerType, true),
//      ("mm", IntegerType, true),
//      ("dd", IntegerType, true),
//      ("evo1", StringType, true),
//      ("geonameid2", LongType, true)
//    )
//
//    var silverTable = spark.createDF(
//      List(
//        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4,"evo1", 1L),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 3, null, null),
//        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4, "evo1", 2L),
//        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4, "evo1", 3L)
//      ),
//      silverSchema
//    )
//
//    silverDF =
//      silverDF
//        .drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid)
//    silverDF =
//      silverDF
//        .select(silverDF.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    silverTable =
//      silverTable
//        .select(silverTable.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    assertSmallDatasetEquality(silverDF, silverTable, orderedComparison = false)
//
//    var silverHistoricalTable = spark.createDF(
//      List(
//        ("Alicante",spain,"Valencia",2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 3, null, null),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 3, null, null),
//        ("Ciudad Real",spain,"Castille-La Mancha",2519402L, Timestamp.valueOf("2020-01-01 01:00:00"), 2020, 1, 3, null, null),
//        ("Alicante", spain, "Comunidad Valenciana", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4, "evo1", 1L),
//        ("Ciudad Real", spain, "Castilla La Mancha", 2519402L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4, "evo1", 2L),
//        ("Tomelloso", spain, "Castilla La Mancha", 2510392L, Timestamp.valueOf("2020-01-02 01:00:00"), 2020, 1, 4, "evo1", 3L)
//      ),
//      silverSchema
//    )
//
//    silverDFHistorical =
//      silverDFHistorical
//        .drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid)
//    silverDFHistorical =
//      silverDFHistorical
//        .select(silverDFHistorical.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    silverHistoricalTable =
//      silverHistoricalTable
//        .select(silverHistoricalTable.columns.sortBy(c=>c).map(c=>col(c)): _*)
//
//    assertSmallDatasetEquality(silverDFHistorical, silverHistoricalTable, orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv partitionby field in file") {
//    val filePath = "pending/source1/20200101_partitionfield_worldCities.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/partitionfield-worldcities/2020/01/01/20200101_partitionfield_worldCities.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/partitionfield_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/partitionfield_worldcities" + historicalSuffix
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.partitionfield_worldcities")
//    val silverDFHistorical = spark.sql("select * from database1.partitionfield_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("partitionfield_worldcities"),None,Some(0L),Some
//      ("partitionfield_worldcities_historical"),None,
//        Some(0L))))
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 3)
//    assert(silverDFHistorical.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//
//    val spain = "Spain"
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true)
//    )
//
//    val silverTable = spark.createDF(
//      List(
//        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00")),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00")),
//        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-01-01 01:00:00"))
//      ),
//      silverSchema
//    )
//
//    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv complex file pattern") {
//    val filePath = "pending/source1/20200101_filepattern_worldCities_20200720072016_I.csv"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/filepattern-worldcities/2020/01/01/20200720072016/I/20200101_filepattern_worldCities_20200720072016_I.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/filepattern_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/filepattern_worldcities" + historicalSuffix
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.filepattern_worldcities")
//    val silverDFHistorical = spark.sql("select * from database1.filepattern_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("filepattern_worldcities"),None,Some(0L),Some
//      ("filepattern_worldcities_historical"),None,
//        Some(0L))))
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 3)
//    assert(silverDFHistorical.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//
//    val spain = "Spain"
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true)
//    )
//
//    val silverTable = spark.createDF(
//      List(
//        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00")),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00")),
//        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-01-01 01:00:00"))
//      ),
//      silverSchema
//    )
//
//    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# csv complex file pattern gz") {
//    val filePath = "pending/source1/20200101_filepattern_worldCities_20200720072016_I.csv.gz"
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(filePath, adfRunId)
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/filepattern-gz-worldcities/2020/01/01/20200720072016/I/20200101_filepattern_worldCities_20200720072016_I.csv.gz"
//
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/filepattern_gz_worldcities"
//    val expectedSilverPathHistorical = this.indationProperties.datalake.basePath + "silver/public/database1/filepattern_gz_worldcities" +
//      historicalSuffix
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.filepattern_gz_worldcities")
//    val silverDFHistorical = spark.sql("select * from database1.filepattern_gz_worldcities" + historicalSuffix)
//
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath), Some(expectedSilverPath), Some(3), Some(0), Some(3),
//      Some(0), Some(ActivitySilverPersistence(Some("database1"),Some("filepattern_gz_worldcities"),None,Some(0L),Some
//      ("filepattern_gz_worldcities_historical"),None,
//        Some(0L))))
//
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 3)
//    assert(silverDFHistorical.count() == 3)
//    assert(Files.exists(Paths.get(expectedSilverPathHistorical)))
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//
//    val spain = "Spain"
//    val silverSchema = List(
//      ("name", StringType, true),
//      ("country", StringType, true),
//      ("subcountry", StringType, true),
//      ("geonameid", LongType, true),
//      ("timeStamp", TimestampType, true)
//    )
//
//    val silverTable = spark.createDF(
//      List(
//        ("Alicante", spain, "Valencia", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00")),
//        ("Cáceres", spain, "Extremadura", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00")),
//        ("Ciudad Real", spain, "Castille-La Mancha", 2519402L, Timestamp.valueOf("2020-01-01 01:00:00"))
//      ),
//      silverSchema
//    )
//
//    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeStorageDay).drop(dataLakeIngestionUuid), silverTable, orderedComparison = false)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# full dataset ingestion") {
//    val metadataManager = new MetadataFilesManager(indationProperties)
//    val dataset = metadataManager.datasetByName("full-snapshot-worldcities")
//    assert(dataset.isDefined)
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(dataset.get, adfRunId)
//
//    val filePath = "pending/source1/20210101_worldCities.csv"
//
//    val notExpectedSchemaMismatchPath = this.indationProperties.landing.basePath  +
//      "schema-mismatch/source1/full-snapshot-worldcities/2021/01/01/20210101_worldCities.csv.bz2"
//
//    val notExpectedInvalidPath = this.indationProperties.landing.basePath  +
//      "invalid/source1/full-snapshot-worldcities/2021/01/01/20210101_worldCities.csv"
//
//    val expectedBronzePath = this.indationProperties.datalake.basePath +
//      "bronze/source1/full-snapshot-worldcities/2021/01/01/20210101_worldCities.csv.gz"
//    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/database1/full_snapshot_worldcities"
//
//    val bronzeDF = spark.read.option("header","true").csv(expectedBronzePath)
//    val silverDF = spark.sql("select * from database1.full_snapshot_worldcities where yyyy=2021")
//
//    // el apartado de validación de estadísticas es irreal, ya que depende del orden en que decida leer los ficheros
//    /*
//    val stats = this.ingestionStats()
//    val activityStatistics = stats.head
//
//    assertActivityStatistics(activityStatistics, filePath, ActivityResults.Ingested_Without_Errors,None,
//      None, None, None, Some(expectedBronzePath),
//      Some(expectedSilverPath), Some(23018), Some(0), Some(23018), Some(0),
//      Some(ActivitySilverPersistence(Some("database1"),Some("full_snapshot_worldcities"),None,Some(0L),Some("full_snapshot_worldcities_historical"),None,
//        Some(0L))))
//
//    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))
//
//    assertActivityDataset(activityStatistics, "full-snapshot-worldcities", IngestionTypes.FullSnapshot, ValidationTypes.FailFast)
//*/
//
//    assert(Files.notExists(Paths.get(notExpectedSchemaMismatchPath)))
//    assert(Files.notExists(Paths.get(notExpectedInvalidPath)))
//    assert(Files.exists(Paths.get(expectedBronzePath)))
//    assert(bronzeDF.count() == 23018)
//    assert(Files.exists(Paths.get(expectedSilverPath)))
//    assert(silverDF.count() == 23018)
//    assertInfoColumns(silverDF)
//    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
//  }
//
//  test("IngestBatchFileActivityParquetOutput# dim_mando dataset ingestion") {
//    val metadataManager = new MetadataFilesManager(indationProperties)
//    val dataset = metadataManager.datasetByName("dim_mando")
//    assert(dataset.isDefined)
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(dataset.get, adfRunId)
//    assert(!spark.sql("select * from slv_personas.dim_mando").isEmpty)
//  }
//
//  test("IngestBatchFileActivityParquetOutput# dim_empleado dataset ingestion") {
//    val metadataManager = new MetadataFilesManager(indationProperties)
//    val dataset = metadataManager.datasetByName("dim_empleado")
//    assert(dataset.isDefined)
//    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
//    ingestBatchActivity.execute(dataset.get, adfRunId)
//    assert(!spark.sql("select * from slv_personas.dim_empleado").isEmpty)
//  }
//
//}
