package com.minsait.indation.bronze

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.bronze.validators.BronzeCsvValidator
import com.minsait.indation.datalake.LandingFileManager
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType
import com.minsait.indation.metadata.models.enums.CsvHeaderTypes.CsvHeaderType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, PermissiveThresholdTypes, ValidationTypes}
import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.Timestamp

class BronzeValidatorSpec extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper {

  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

  class Fixture(val indationProperties: IndationProperties) extends ConfigurationReader with LandingFileManager

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  lazy val fixture = new Fixture(IndationProperties(
    MetadataProperties("", "", "", ""),
    Some(DatabricksProperties(DatabricksSecretsProperties(""))),
    LandingProperties("AccountName1", testResourcesBasePath + "landing/", "landing", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming"),
    DatalakeProperties("", "", ""),
    SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties()),
    Local,
    tmpDirectory
  ))

  override def beforeAll() {
    SparkSessionFactory.configSparkSession(fixture.indationProperties)
  }

  test("BronzeValidatorSpec#Validate validate a correct bronze DataFrame") {

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "", fixed = None
        , csv = Option(CsvFormat("UTF-8", ",", CsvHeaderType("first_line"), None, Some(false), None, None)), json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List(
        Column("name", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("country", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("subcountry", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("geonameid", ColumnsType("integer"), None, None, None, None, None, None, None, sensitive = false)
      ))), None, None)

    val filePath = testResourcesBasePath + "landing/pending/source1/world-cities.csv"

    val readFileDF = spark.read
      .option("header", value = true)
      .option("sep", ",")
      .csv(filePath)

    val validDF = readFileDF.withColumn("_corrupt_record", lit(null))

    val (isValidFile, _, bronzeInvalidDf) =
      BronzeCsvValidator.validate(fixture.indationProperties, filePath, validDF, testDataset, Option(""))

    assert(isValidFile && bronzeInvalidDf.isEmpty)
  }

  test("BronzeValidatorSpec#Validate validate a bronze DataFrame with wrong rows over threshold") {

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "", fixed = None
        , csv = Option(CsvFormat("UTF-8", ",", CsvHeaderType("first_line"), None, Some(false), None, None)), json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.Permissive
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List(
        Column("name", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("country", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("subcountry", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("geonameid", ColumnsType("integer"), None, None, None, None, None, None, None, sensitive = false)
      ))), None, None)

    val filePath = testResourcesBasePath + "landing/pending/source1/world-cities.csv"

    val readFileDF = spark.read
      .option("header", value = true)
      .option("sep", ",")
      .csv(filePath)

    val validDF = readFileDF.withColumn("_corrupt_record", lit(1))

    val (isValidFile, _, bronzeInvalidDf) =
      BronzeCsvValidator.validate(fixture.indationProperties, filePath, validDF, testDataset, Option(""))

    assert(!isValidFile)
  }

  test("BronzeValidatorSpec#Validate validate a DataFrame with invalid Schema") {

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "", fixed = None
        , csv = Option(CsvFormat("UTF-8", ",", CsvHeaderType("first_line"), None, Some(false), None, None)), json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List(
        Column("name", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("country", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("subcountry", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false),
        Column("geonameid", ColumnsType("integer"), None, None, None, None, None, None, None, sensitive = false),
        Column("badColumnName", ColumnsType("string"), None, None, None, None, None, None, None, sensitive = false)
      ))), None, None)

    val filePath = testResourcesBasePath + "landing/pending/source1/world-cities.csv"

    val readFileDF = spark.read
      .option("header", value = true)
      .option("sep", ",")
      .csv(filePath)

    val validDF = readFileDF.withColumn("_corrupt_record", lit(null))

    val (isValidFile, _, _) =
      BronzeCsvValidator.validate(fixture.indationProperties, filePath, validDF, testDataset, Option(""))

    assert(!isValidFile)
  }
}
