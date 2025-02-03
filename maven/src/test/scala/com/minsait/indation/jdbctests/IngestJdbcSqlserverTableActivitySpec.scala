package com.minsait.indation.jdbctests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.BuildInfo
import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol.activityStatisticsFormat
import com.minsait.indation.activity.statistics.models.{ActivityResults, ActivitySilverPersistence, ActivityStatistics, ActivityTriggerTypes}
import com.minsait.indation.activity.{IngestBatchSourceActivity, IngestJdbcTableActivity}
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.exceptions.SourceException
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, ValidationTypes}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum}
import org.h2.tools.RunScript
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import spray.json.enrichString

import java.io.{File, InputStreamReader}
import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, Timestamp}
import java.util.Properties
import scala.collection.JavaConverters._

class IngestJdbcSqlserverTableActivitySpec extends AnyFunSuite with MockitoSugar  with BeforeAndAfterAll with BeforeAndAfterEach
  with DatasetComparer with SparkSessionWrapper {

  // Separador de carpetas en sistema de archivos
  private val DirectorySeparator: String = "/"

  // Ruta de los recursos en fase de ejecución
  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\",DirectorySeparator) + DirectorySeparator

  // carpeta temporal
  private val tmpDirectory = testResourcesBasePath + "tmp/"

  // otras constantes
  private val dataLakeStorageDate = "datalake_load_date"
  private val dataLakeIngestionUuid = "datalake_ingestion_uuid"
  private val adfRunId = "adf-uuid"
  private val historicalSuffix = "_historical"

  // propiedades de ejecución para los test
  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("",testResourcesBasePath,"","")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/" , "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("",testResourcesBasePath,"")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  /**
   * Método que se ejecuta antes de los tests. Debe inicializar la sesión y los datos de SQL
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)

    // Inicialización de datos en bbdd H2

    // Conexión con bbdd de los tests
    Class.forName("org.h2.Driver")
    val dbConn = DriverManager.getConnection("jdbc:h2:~/AdventureWorks2017;MODE=MSSQLServer;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_UPPER=FALSE;", "sa", "")

    // Limpieza de todos los objetos creados
    val statement = dbConn.prepareStatement("DROP ALL OBJECTS")
    statement.execute()

    // Ejecución de script de inicialización de datos
    val zip1 = new java.util.zip.ZipFile(this.getClass.getResource("/SQL/JdbcSource/jdbcTestPreparation.zip").getPath)
    val zip2 = new java.util.zip.ZipFile(this.getClass.getResource("/SQL/JdbcSource/jdbcTestInsertDetailSmall.zip").getPath)
    zip1.entries.asScala.foreach(script =>        RunScript.execute(dbConn, new InputStreamReader(zip1.getInputStream(script)))    )
    zip2.entries.asScala.foreach(script =>        RunScript.execute(dbConn, new InputStreamReader(zip2.getInputStream(script)))    )
  }

  /**
   * Método que se ejecuta tras la finalización de cada test
   */
  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(Paths.get(testResourcesBasePath + "silver").toFile)
  }

  /**
   * Limpieza de bbdd H2 tras la ejecución de todos los tests
   */
  override def afterAll(): Unit = {
    super.afterAll()
    // Conexión con bbdd de los tests
    Class.forName("org.h2.Driver")
    val dbConn = DriverManager.getConnection("jdbc:h2:~/AdventureWorks2017;MODE=MSSQLServer;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_UPPER=FALSE;", "sa", "")

    // Limpieza de todos los objetos creados
    val statement = dbConn.prepareStatement("DROP ALL OBJECTS")
    statement.execute()
  }

  /**
   * función que devuelve las estadísticas de ejecución
   * @return Lista con las estadísticas de ejecución de actividades
   */
  private def ingestionStats(): List[ActivityStatistics] = {

    spark.sql("select * from applications.indation order by " + dataLakeStorageDate + " desc")
      .toJSON
      .collect
      .toList
      .map(_.parseJson.convertTo[ActivityStatistics])
  }

  /**
   * Método utilizado para validar si el dataframe de salida de una ejecución contiene las columnas de información
   * @param df Dataframe a evaluar
   */
  private def assertInfoColumns(df: DataFrame): Unit = {
    assert(df.columns.contains(dataLakeStorageDate))
    assert(df.columns.contains(dataLakeIngestionUuid))
  }

  /**
   * Método utilizado para validar un paquete de estadísticas de ejecución recibido
   */
  private def assertActivityStatistics(activityStatistics: ActivityStatistics,
                                       result: ActivityResults.ActivityResult,
                                       unknownAbsolutePath: Option[String],
                                       corruptedAbsolutePath: Option[String],
                                       schemaMismatchAbsolutePath: Option[String],
                                       invalidAbsolutePath: Option[String],
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
//    assertResult(filePath)(activityStatistics.origin)
    assertResult(result)(activityStatistics.result)
    assertResult(true)(activityStatistics.execution.start.compareTo(new Timestamp(System.currentTimeMillis)) < 0)
    assertResult(true)(activityStatistics.execution.end.compareTo(new Timestamp(System.currentTimeMillis)) < 0)
    assertResult(true)(activityStatistics.execution.duration > 0)
    assertResult(unknownAbsolutePath.getOrElse(None))(activityStatistics.output_paths.unknown.getOrElse(None))
    assertResult(corruptedAbsolutePath.getOrElse(None))(activityStatistics.output_paths.corrupted.getOrElse(None))
    assertResult(schemaMismatchAbsolutePath.getOrElse(None))(activityStatistics.output_paths.schema_mismatch.getOrElse(None))
    assertResult(invalidAbsolutePath.getOrElse(None))(activityStatistics.output_paths.invalid.getOrElse(None))
//    assertResult(bronzeAbsolutePath.getOrElse(None))(activityStatistics.output_paths.bronze.getOrElse(None))
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

  /**
   * Método utilizado para validar el resultado de una ejecución de un dataset
   */
  private def assertActivityDataset(activityStatistics: ActivityStatistics,
                                    name: String,
                                    ingestion: IngestionTypes.IngestionType,
                                    validation: ValidationTypes.ValidationType,
                                    partition_by: String
                                   ): Unit = {
    assertResult(name)(activityStatistics.dataset.get.name)
    assertResult(DatasetTypes.Table)(activityStatistics.dataset.get.typ)
    assertResult(1)(activityStatistics.dataset.get.version)
    assertResult(ingestion)(activityStatistics.dataset.get.ingestion_mode)
    assertResult(validation)(activityStatistics.dataset.get.validation_mode)
    assertResult(partition_by)(activityStatistics.dataset.get.partition_by)
  }

  // Inicio de tests

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot extra columns ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-extra-columns-ok")
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryFSEC"

    val silverDF = spark.sql("select * from Production.ProductCategoryFSEC")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.Ingested_Without_Errors,None,
      None, None, None, /*Some(expectedBronzePath),*/
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryFSEC"),None,Some(0L),
        Some("ProductCategoryFSEC_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.columns.length == 5) // 2 columnas originales + 3 de indation
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot extra columns error") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-extra-columns-error")
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryFSECE"

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    val expectedBronzePath = "Production.ProductCategory"

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.RejectedSchemaMismatch,
      None, None, Some(expectedBronzePath), None,
      None, Some(0), Some(4), None, None,
      None)

  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot partitioned historical partitions should not be lost") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)

    // Pre-cleaning
    if (spark.catalog.databaseExists("Sales") && spark.catalog.tableExists("Sales.SalesOrderPerDay_historical")) {
      println("Cleaning Sales.SalesOrderPerDay_historical for test")
      spark.sql("DELETE FROM Sales.SalesOrderPerDay_historical")
    }

    // First execution
    println("First execution")
    val dataset1 = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-partition1")
    ingestJdbcTableActivity.execute(dataset1.get, adfRunId, "None")
    val dfResult1 = spark.sql("select * from Sales.SalesOrderPerDay_historical")
    assert(dfResult1.count()==8)
    assertResult("247664.268000")(dfResult1.where("Category = 'Mountain Bikes'").select(col("SumTotal")).collect()(0).getDecimal(0).toString)
    assertResult("503805.916900")(dfResult1.agg(sum(col("SumTotal"))).collect()(0).getDecimal(0).toString)

    // Second execution
    println("Second execution")
    val dataset2 = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-partition2")
    ingestJdbcTableActivity.execute(dataset2.get, adfRunId, "None")
    val dfResult2 = spark.sql("select * from Sales.SalesOrderPerDay_historical")
    assert(dfResult2.count()==8)
    assertResult("6774.980000")(dfResult2.where("Category = 'Mountain Bikes'").select(col("SumTotal")).collect()(0).getDecimal(0).toString)
    assertResult("50028.300000")(dfResult2.agg(sum(col("SumTotal"))).collect()(0).getDecimal(0).toString)

  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot fail-fast with date and datetime ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-date")
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Sales/SalesOrderDay"

    val silverDF = spark.sql("select * from Sales.SalesOrderDay")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.Ingested_Without_Errors,None,
      None, None, None, /*Some(expectedBronzePath),*/
      Some(expectedSilverPath), Some(1000), Some(0), Some(1000), Some(0),
      Some(ActivitySilverPersistence(Some("Sales"),Some("SalesOrderDay"),None,Some(0L),
        Some("SalesOrderDay_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-sqlserver-fullsnapshot-date", IngestionTypes.FullSnapshot, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 1000)
    assertInfoColumns(silverDF)
  }


  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot fail-fast ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot")
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryFS"

    val silverDF = spark.sql("select * from Production.ProductCategoryFS")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.Ingested_Without_Errors,None,
      None, None, None, /*Some(expectedBronzePath),*/
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryFS"),None,Some(0L),
        Some("ProductCategoryFS_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-sqlserver-fullsnapshot", IngestionTypes.FullSnapshot, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 4)
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot fail-fast by url ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-url")
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryFSURL"

    val silverDF = spark.sql("select * from Production.ProductCategoryFSURL")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.Ingested_Without_Errors,None,
      None, None, None, /*Some(expectedBronzePath),*/
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryFSURL"),None,Some(0L),
        Some("ProductCategoryFSURL_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-sqlserver-fullsnapshot-url", IngestionTypes.FullSnapshot, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 4)
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot from query fail-fast ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-fromquery")
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryFSVW"

    val silverDF = spark.sql("select * from Production.ProductCategoryFSVW")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.Ingested_Without_Errors,None,
      None, None, None, /*Some(expectedBronzePath),*/
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryFSVW"),None,Some(0L),
        Some("ProductCategoryFSVW_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(activityStatistics, "dataset-sqlserver-fullsnapshot-fromquery", IngestionTypes.FullSnapshot, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 4)
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full-snapshot fail-fast bad schema") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("dataset-sqlserver-fullsnapshot-badschema")

    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, ActivityResults.RejectedSchemaMismatch,
      None, None, Some("Production.ProductCategory"), None,
      None, Some(0), Some(4),
      None, None, None)

  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion incremental fail-fast ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)

    // iniciamos cargando los primeros registros con el dataset fullsnapshot
    val dataset1 = metadataReader.datasetByName("dataset-sqlserver-incremental-previous")
    ingestJdbcTableActivity.execute(dataset1.get, adfRunId, "None")

    // cargamos ahora el dataset con los valores incrementales
    val dataset2 = metadataReader.datasetByName("dataset-sqlserver-incremental")
    ingestJdbcTableActivity.execute(dataset2.get, adfRunId, "None")

    // iniciamos validación
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryI"

    val silverDF = spark.sql("select * from Production.ProductCategoryI")

    val stats = this.ingestionStats()

    assertActivityStatistics(stats.last, ActivityResults.Ingested_Without_Errors,None,
      None, None, None,
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryI"),None,Some(0),
        None,None,None)))

    assertActivityStatistics(stats.head, ActivityResults.Ingested_Without_Errors,None,
      None, None, None,
      Some(expectedSilverPath), Some(1), Some(0), Some(1), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryI"),Some(0),Some(1),
        None,None,None)))

    assertActivityDataset(stats.head, "dataset-sqlserver-incremental", IngestionTypes.Incremental, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 5)
    assertInfoColumns(silverDF)
  }


  test("IngestBatchJdbcTableActivity#sqlserver ingestion last changes with allowPartitionChange=false fail-fast ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)

    // iniciamos cargando los primeros registros con el dataset fullsnapshot
    val dataset1 = metadataReader.datasetByName("dataset-sqlserver-lastchanges-previous")
    ingestJdbcTableActivity.execute(dataset1.get, adfRunId, "None")

    // cargamos ahora el dataset con los valores incrementales
    val dataset2 = metadataReader.datasetByName("dataset-sqlserver-lastchanges")
    ingestJdbcTableActivity.execute(dataset2.get, adfRunId, "None")

    // iniciamos validación
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryLC"

    val silverDF = spark.sql("select * from Production.ProductCategoryLC")

    val stats = this.ingestionStats()

    assertActivityStatistics(stats.last, ActivityResults.Ingested_Without_Errors,None,
      None, None, None,
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryLC"),None,Some(0L),
        Some("ProductCategoryLC_historical"),None,Some(0L))))

    assertActivityStatistics(stats.head, ActivityResults.Ingested_Without_Errors,None,
      None, None, None,
      Some(expectedSilverPath), Some(2), Some(0), Some(2), Some(0),
      Some(ActivitySilverPersistence(Some("Production"),Some("ProductCategoryLC"),None,Some(0L),
        Some("ProductCategoryLC_historical"),Some(0L),Some(1L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(stats.last.output_paths.silver_historical.getOrElse(None))

    assertActivityDataset(stats.head, "dataset-sqlserver-lastchanges", IngestionTypes.LastChanges, ValidationTypes.FailFast, "ProductCategoryID")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 5)
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion full source ok"){
    val sourceName = "source-sqlserver-full"
    val metadataManager = new MetadataFilesManager(indationProperties)
    val source = metadataManager.sourceByName(sourceName)

    if (source.isEmpty) {
      throw new SourceException("Source " + sourceName + " not found.")
    }

    IngestBatchSourceActivity.initJdbc(source.get, indationProperties, "-1")

    //TODO: Asserts
  }

  def testOdsGenerico(datasetName: String, numColumns: Integer): Unit = {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName(datasetName)
    ingestJdbcTableActivity.execute(dataset.get, adfRunId, "None")

    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/slv_comunicaciones/registro_llamadas"

    val silverDF = spark.sql("select * from slv_comunicaciones.registro_llamadas")

    val stats = this.ingestionStats()
    val activityStatistics = stats.head

    assertActivityStatistics(activityStatistics, /*filePath,*/ ActivityResults.Ingested_Without_Errors,None,
      None, None, None, /*Some(expectedBronzePath),*/
      Some(expectedSilverPath), Some(1000), Some(0), Some(1000), Some(0),
      Some(ActivitySilverPersistence(Some("slv_comunicaciones"),Some("registro_llamadas"),None,Some(0L),
        Some("registro_llamadas_historical"),None,Some(0L))))

    assertResult(expectedSilverPath + this.historicalSuffix)(activityStatistics.output_paths.silver_historical.getOrElse(None))

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 1000)
    assert(silverDF.columns.length == numColumns) // 270 de origen + las nuevas columnas
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion incremental isTimestamp with alias fail-fast ok") {

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)

    // iniciamos cargando los primeros registros con el dataset fullsnapshot
    val dataset1 = metadataReader.datasetByName("dataset-sqlserver-incremental-isTimestamp-alias-previous")
    ingestJdbcTableActivity.execute(dataset1.get, adfRunId, "None")

    // cargamos ahora el dataset con los valores incrementales
    val dataset2 = metadataReader.datasetByName("dataset-sqlserver-incremental-isTimestamp-alias")
    ingestJdbcTableActivity.execute(dataset2.get, adfRunId, "None")

    // iniciamos validación
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryI_isTimestamp_alias"

    val silverDF = spark.sql("select * from Production.ProductCategoryI_isTimestamp_alias")

    val stats = this.ingestionStats()

    assertActivityStatistics(stats.last, ActivityResults.Ingested_Without_Errors, None,
      None, None, None,
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"), Some("ProductCategoryI_isTimestamp_alias"), None, Some(0),
        None, None, None)))

    assertActivityStatistics(stats.head, ActivityResults.Ingested_Without_Errors, None,
      None, None, None,
      Some(expectedSilverPath), Some(1), Some(0), Some(1), Some(0),
      Some(ActivitySilverPersistence(Some("Production"), Some("ProductCategoryI_isTimestamp_alias"), Some(0), Some(1),
        None, None, None)))

    assertActivityDataset(stats.head, "dataset-sqlserver-incremental-isTimestamp-alias", IngestionTypes.Incremental, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 5)
    assertInfoColumns(silverDF)
  }

  test("IngestBatchJdbcTableActivity#sqlserver ingestion incremental isTimestamp with alias onlyNewData fail-fast ok") {

    val connectionProperties = new Properties
    connectionProperties.setProperty("Driver", "org.h2.Driver")
    connectionProperties.put("user", "sa")
    connectionProperties.put("password", "")

    val url = "jdbc:h2:~/AdventureWorks2017;MODE=MSSQLServer;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_UPPER=FALSE;"
    val productionProductCategory = spark
      .read
      .jdbc(url,
        "Production.ProductCategory",
        connectionProperties
      )

    val productionProductCategoryIncremental = spark
      .read
      .jdbc(url,
        "Production.ProductCategoryIncremental",
        connectionProperties
      )

    val ingestJdbcTableActivity = new IngestJdbcTableActivity(indationProperties)
    val metadataReader = new MetadataFilesManager(indationProperties)

    // iniciamos cargando los primeros registros con el dataset fullsnapshot
    val dataset1 = metadataReader.datasetByName("dataset-sqlserver-incremental-isTimestamp-alias-onlyNewData-previous")
    ingestJdbcTableActivity.execute(dataset1.get, adfRunId, "None")

/*    val df1 = spark.sql("select * from Production.ProductCategoryI_isTimestamp_alias_onlyNewData")
    df1.show(100,false)
    spark.sql("delete from Production.ProductCategoryI_isTimestamp_alias_onlyNewData")
    val df2 = spark.sql("select * from Production.ProductCategoryI_isTimestamp_alias_onlyNewData")
    df2.show(100,false)*/

    // cargamos ahora el dataset con los valores incrementales
    val dataset2 = metadataReader.datasetByName("dataset-sqlserver-incremental-isTimestamp-alias-onlyNewData")
    ingestJdbcTableActivity.execute(dataset2.get, adfRunId, "None")

    // iniciamos validación
    val expectedSilverPath = this.indationProperties.datalake.basePath + "silver/public/Production/ProductCategoryI_isTimestamp_alias_onlyNewData"

    val silverDF = spark.sql("select * from Production.ProductCategoryI_isTimestamp_alias_onlyNewData")

    val stats = this.ingestionStats()

    assertActivityStatistics(stats.last, ActivityResults.Ingested_Without_Errors, None,
      None, None, None,
      Some(expectedSilverPath), Some(4), Some(0), Some(4), Some(0),
      Some(ActivitySilverPersistence(Some("Production"), Some("ProductCategoryI_isTimestamp_alias_onlyNewData"), None, Some(0),
        None, None, None)))

    assertActivityStatistics(stats.head, ActivityResults.Ingested_Without_Errors, None,
      None, None, None,
      Some(expectedSilverPath), Some(1), Some(0), Some(1), Some(0),
      Some(ActivitySilverPersistence(Some("Production"), Some("ProductCategoryI_isTimestamp_alias_onlyNewData"), Some(0), Some(1),
        None, None, None)))

    assertActivityDataset(stats.head, "dataset-sqlserver-incremental-isTimestamp-alias-onlyNewData", IngestionTypes.Incremental, ValidationTypes.FailFast, "")

    assert(Files.exists(Paths.get(expectedSilverPath)))
    assert(silverDF.count() == 5)
    assertInfoColumns(silverDF)
  }

}
