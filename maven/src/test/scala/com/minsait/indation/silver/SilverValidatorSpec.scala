package com.minsait.indation.silver

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.activity.IngestBatchFileActivity
import com.minsait.indation.datalake.LandingFileManager
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.silver.validators.SilverValidator
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.Timestamp

class SilverValidatorSpec extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper {

  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

  class Fixture(val indationProperties: IndationProperties) extends ConfigurationReader with LandingFileManager

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  lazy val fixture = new Fixture(IndationProperties(
    MetadataProperties("", testResourcesBasePath, "", ""),
    Some(DatabricksProperties(DatabricksSecretsProperties(""))),
    LandingProperties("AccountName1", testResourcesBasePath + "landing/", "landing", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming"),
    DatalakeProperties("", testResourcesBasePath, ""),
    SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties()),
    Local,
    tmpDirectory
  ))

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

  private val adfRunId = "adf-uuid"


  override def beforeAll() {
    SparkSessionFactory.configSparkSession(fixture.indationProperties)
  }

  test("SilverValidatorSpec#Validate validate a correct DataFrame") {

    import spark.implicits._

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), Some(SchemaColumns(List(
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)
      ))), None, None)

    val validDF = Seq(
      ("A", "B", "C")
    ).toDF("String", "String", "String")

    val emptyDF = spark.emptyDataFrame

    assert(SilverValidator.validate(testDataset, validDF, emptyDF, spark.emptyDataFrame))
  }

  test("SilverValidatorSpec#Validate validate a DataFrame with invalid rows in FAIL_FAST mode") {

    import spark.implicits._

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), Some(SchemaColumns(List(
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)))),
      None, None)

    val validDF = Seq(
      ("A", "B", "C")
    ).toDF("String", "String", "String")

    val invalidDf = Seq(
      ("null", "null", "null")
    ).toDF("String", "String", "String")

    assert(!SilverValidator.validate(testDataset, validDF, invalidDf, spark.emptyDataFrame))
  }

  test("SilverValidatorSpec#Validate validate a DataFrame with invalid rows over threshold in Permissive mode") {

    import spark.implicits._

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), Some(SchemaColumns(List(
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)))),
      None, None)

    val validDF = Seq(
      ("A", "B", "C")
    ).toDF("String", "String", "String")

    val invalidDf = Seq(
      ("null", "null", "null"),
      ("null", "null", "null"),
      ("null", "null", "null")
    ).toDF("String", "String", "String")

    assert(!SilverValidator.validate(testDataset, validDF, invalidDf, spark.emptyDataFrame))
  }
  test("SilverValidatorSpec#SilverValidator method validateQuality applies completenessRule") {
    //Creamos la data de entrada
    val schema = StructType(Array(
      StructField("Salarios", IntegerType, true),
      StructField("Edad", IntegerType, true),
      StructField("Pais", StringType, true),
      StructField("Fecha", StringType, true)
    ))

    val data = Seq(
      Row(4000, 50, "ES", "2020-12-18"),
      Row(4000, 50, "AR", "2020-12-18"),
      Row(4000, 50, "PE", "2020-12-18"),
      Row(300, 30, "MX", "2020-12-18"),
      Row(null, 70, "CO", "2020-10-18"))

    val dataframeOK1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Creamos la data que esperamos
    val datakOExpected = Seq(
      Row(null, 70, "CO", "2020-10-18"))

    val dataOkExpected = Seq(
      Row(300, 30, "MX", "2020-12-18"),
      Row(4000, 50, "ES", "2020-12-18"),
      Row(4000, 50, "PE", "2020-12-18"),
      Row(4000, 50, "AR", "2020-12-18"))

    val QualityOkExpected = spark.createDataFrame(spark.sparkContext.parallelize(dataOkExpected), schema)
    val QualitykOExpected = spark.createDataFrame(spark.sparkContext.parallelize(datakOExpected), schema)

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), Some(SchemaColumns(List(
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)))),
      None, Some(QualityRules(None, notNullRule = Some(List("salarios", "edad")), None,
        None,RuleWarning)))

    //Llamando al validate Quality obtenemos dos dataframes, uno con registros OK y otro con registros KO
    val Qualities = SilverValidator.validateQuality(testDataset, dataframeOK1)

    assert(Qualities._1.except(QualityOkExpected).isEmpty)
    assert(Qualities._2.except(QualitykOExpected).isEmpty)
  }

  test("SilverValidatorSpec#SilverValidator method validateQuality applies uniquenessRule") {
    //Creamos la data de entrada
    val schema = StructType(Array(
      StructField("Salarios", IntegerType, true),
      StructField("Edad", IntegerType, true),
      StructField("Pais", StringType, true),
      StructField("Fecha", StringType, true)

    ))

    val data = Seq(
      Row(4000, 50, "ES", "2020-12-18"),
      Row(4000, 50, "AR", "2020-12-18"),
      Row(4000, 50, "PE", "2020-12-18"),
      Row(300, null, "MX", "2020-12-18"),
      Row(null, 70, "CO", "2020-10-18"))

    val dataframeOK1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Creamos la data que esperamos
    val dataOkExpected = Seq(
      Row(4000, 50, "ES", "2020-12-18"),
      Row(300, null, "MX", "2020-12-18"),
      Row(null, 70, "CO", "2020-10-18"))

    val datakOExpected = Seq(
      Row(4000, 50, "PE", "2020-12-18"),
      Row(4000, 50, "AR", "2020-12-18"))

    val QualityOkExpected = spark.createDataFrame(spark.sparkContext.parallelize(dataOkExpected), schema)
    val QualitykOExpected = spark.createDataFrame(spark.sparkContext.parallelize(datakOExpected), schema)

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), Some(SchemaColumns(List(
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)))),
      None, Some(QualityRules(Some(UniquenessRules(List(UniquenessRule("uniqueRule1", List("salarios", "edad")),
        UniquenessRule("uniqueRule2", List("Pais"))))), None,
        None, None,RuleWarning)))

    //Llamando al validate Quality obtenemos dos dataframes, uno con registros OK y otro con registros KO
    val Qualities = SilverValidator.validateQuality(testDataset, dataframeOK1)

    assert(Qualities._1.except(QualityOkExpected).isEmpty)
    assert(Qualities._2.except(QualitykOExpected).isEmpty)
  }
  test("SilverValidatorSpec#SilverValidator method validateQuality applies expressionRule") {
    //Creamos la data de entrada
    val schema = StructType(Array(
      StructField("Salarios", IntegerType, true),
      StructField("Edad", IntegerType, true),
      StructField("Pais", StringType, true),
      StructField("Fecha", StringType, true)

    ))

    val data = Seq(
      Row(4000, 50, "ES", "2020-12-18"),
      Row(4000, 50, "AR", "2020-12-18"),
      Row(4000, 40, "PE", "2020-12-18"),
      Row(300, null, "MX", "2020-12-18"),
      Row(null, 70, "CO", "2020-10-18"))

    val dataframeOK1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Creamos la data que esperamos
    val dataOkExpected = Seq(
      Row(4000, 50, "ES", "2020-12-18"),
      Row(4000, 50, "AR", "2020-12-18"))

    val datakOExpected = Seq(
      Row(300, null, "MX", "2020-12-18"),
      Row(null, 70, "CO", "2020-10-18"),
      Row(4000, 40, "PE", "2020-12-18")
    )

    val QualityOkExpected = spark.createDataFrame(spark.sparkContext.parallelize(dataOkExpected), schema)
    val QualitykOExpected = spark.createDataFrame(spark.sparkContext.parallelize(datakOExpected), schema)

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), Some(SchemaColumns(List(
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("String", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)))),
      None, Some(QualityRules(None, None, None, Some(ExpressionRules(List(expressionRule("expr1", "Salarios > 300"),
        expressionRule("expr2", "Edad>40")))),RuleWarning)))

    //Llamando al validate Quality obtenemos dos dataframes, uno con registros OK y otro con registros KO
    val Qualities = SilverValidator.validateQuality(testDataset, dataframeOK1)

    assert(Qualities._1.except(QualityOkExpected).isEmpty)
    assert(Qualities._2.except(QualitykOExpected).isEmpty)
  }
  test("SilverValidatorSpec#SilverValidator method validateQuality applies IntegrityRule") {
    val filePath = "pending/source1/20200110_last_changes_worldCities.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    //Creamos la data de entrada
    val schema = StructType(Array(
      StructField("Salarios", IntegerType, true),
      StructField("Edad", IntegerType, true),
      StructField("Pais", StringType, true),
      StructField("Fecha", StringType, true),
      StructField("nombre", StringType, true),
      StructField("geonameid", LongType, true)
    ))

    val data = Seq(
      Row(4000, 50, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(4000, 50, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(4000, 50, "Spain", "2020-12-18","Alicante",2520611L),
      Row(300, null, "France", "2020-12-18","Cáceres",2521978L),
      Row(null, 70, "Spain", "2020-10-18", "Las Palmas",2521978L))

    val dataframeOK1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Creamos la data que esperamos
    val dataOkExpected = Seq(
      Row(4000, 50, "Spain", "2020-12-18","Alicante",2520611L))

    val datakOExpected = Seq(
      Row(4000, 50, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(300, null, "France", "2020-12-18","Cáceres",2521978L),
      Row(null, 70, "Spain", "2020-10-18", "Las Palmas",2521978L),
      Row(4000, 50, "Spain", "2020-12-18","Ciudad Real",1111111L)
    )

    val QualityOkExpected = spark.createDataFrame(spark.sparkContext.parallelize(dataOkExpected), schema)
    val QualitykOExpected = spark.createDataFrame(spark.sparkContext.parallelize(datakOExpected), schema)

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "database1", "last_changes_worldcities", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), None,
      None, Some(QualityRules(None,None,Some(IntegrityRules(List(IntegrityRule("Integrity1",List("nombre","Pais"),
        "database1.last_changes_worldcities",List("name","country")),IntegrityRule("Integrity2",List("geonameid"),
        "database1.last_changes_worldcities",List("geonameid"))))), None,RuleWarning)))

    //Llamando al validate Quality obtenemos dos dataframes, uno con registros OK y otro con registros KO
    val Qualities = SilverValidator.validateQuality(testDataset, dataframeOK1)

    assert(Qualities._1.except(QualityOkExpected).isEmpty)
    assert(Qualities._2.except(QualitykOExpected).isEmpty)
  }
  test("SilverValidatorSpec#SilverValidator method validateQuality applies allRules") {
    val filePath = "pending/source1/20200105_last_changes_worldCities.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    //Creamos la data de entrada
    val schema = StructType(Array(
      StructField("salarios", IntegerType, true),
      StructField("edad", IntegerType, true),
      StructField("country", StringType, true),
      StructField("fecha", StringType, true),
      StructField("nombre", StringType, true),
      StructField("geonameid", LongType, true)
    ))

    val data = Seq(
      Row(4000, 32, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(4000, 31, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(4000, 30, "Spain", "2020-12-18","Alicante",2520611L),
      Row(4000, 33, "Spain", "2020-12-18","Alicante",2520611L),
      Row(4000, 37, "France", "2020-12-18","Alicante",2520611L),
      Row(4000, 35, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(4000, 35, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(null, 35, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(5000, 50, "Spain", "2020-12-18","Alicante",2520611L),
      Row(5000, 50, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(5000, 50, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(5000, 50, "Spain", "2020-12-18","Alicante",2520611L))

    val dataframeOK1 = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //Creamos la data que esperamos
    val dataOkExpected = Seq(
      Row(4000, 32, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(4000, 31, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(4000, 30, "Spain", "2020-12-18","Alicante",2520611L),
      Row(4000, 33, "Spain", "2020-12-18","Alicante",2520611L),
      Row(4000, 35, "Spain", "2020-12-18","Tomelloso",2520611L)
    )

    val datakOExpected = Seq(
      Row(4000, 37, "France", "2020-12-18","Alicante",2520611L),
      Row(4000, 35, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(null, 35, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(5000, 50, "Spain", "2020-12-18","Alicante",2520611L),
      Row(5000, 50, "Spain", "2020-12-18","Tomelloso",2520611L),
      Row(5000, 50, "Spain", "2020-12-18","Ciudad Real",1111111L),
      Row(5000, 50, "Spain", "2020-12-18","Alicante",2520611L)
    )

    val QualityOkExpected = spark.createDataFrame(spark.sparkContext.parallelize(dataOkExpected), schema)
    val QualitykOExpected = spark.createDataFrame(spark.sparkContext.parallelize(datakOExpected), schema)

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType(""), filePattern = "", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "database1", "last_changes_worldcities", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType("json-columns"), None,
      None, Some(QualityRules(Some(UniquenessRules(List(UniquenessRule("uniqueRule1", List("salarios", "edad"))))),
        Some(List("salarios")),Some(IntegrityRules(List(IntegrityRule("Integrity1",List("country"),
        "database1.last_changes_worldcities",List("country"))))),
        Some(ExpressionRules(List(expressionRule("expr1", "salarios < 5000")))),RuleWarning)))

    //Llamando al validate Quality obtenemos dos dataframes, uno con registros OK y otro con registros KO
    val Qualities = SilverValidator.validateQuality(testDataset, dataframeOK1)

    assert(Qualities._1.except(QualityOkExpected).isEmpty)
    assert(Qualities._2.except(QualitykOExpected).isEmpty)
  }
}