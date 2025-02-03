package com.minsait.indation.datalake

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.datalake.writers.BronzeFileWriter
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.IngestionTypes.FullSnapshot
import com.minsait.indation.metadata.models.enums.PermissiveThresholdTypes.Absolute
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.ValidationTypes.FailFast
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Dataset, FileInput, SchemaColumns}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

class BronzeFileWriterSpec extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper {

  import spark.implicits._

  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

  class Fixture(val indationProperties: IndationProperties) extends ConfigurationReader with BronzeFileWriter with SparkSessionWrapper

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  lazy val fixture = new Fixture(IndationProperties(
    MetadataProperties("", "", "", ""),
    Some(DatabricksProperties(DatabricksSecretsProperties(""))),
    LandingProperties("AccountName1", testResourcesBasePath + "landing/", "landing", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming"),
    DatalakeProperties("storageAccount1", testResourcesBasePath, "container1"),
    SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties()),
    Local,
    tmpDirectory
  ))

  override def beforeAll() {
    SparkSessionFactory.configSparkSession(fixture.indationProperties)
  }

  test("BronzeFileWriter#write write json dataframe compressed in file") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "sourceName1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("json"), filePattern = "<yyyy><mm><dd>_file.json", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), FullSnapshot
      , "", "", FailFast
      , 2, Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "20200101_file.json"
    val expectedPath = testResourcesBasePath + "bronze/sourceName1/dataset1/2020/01/01/20200101_file.json.bz2"

    fixture.writeBronze(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))
  }

  test("BronzeFileWriter#write write csv dataframe compressed in file") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "sourceName1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "20200101_file.csv"
    val expectedPath = testResourcesBasePath + "bronze/sourceName1/dataset1/2020/01/01/20200101_file.csv.bz2"

    fixture.writeBronze(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))
  }

  test("BronzeFileWriter#write write orc dataframe compressed in file") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "sourceName1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("orc"), filePattern = "<yyyy><mm><dd>_file.orc", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "20200101_file.orc"
    val expectedPath = testResourcesBasePath + "bronze/sourceName1/dataset1/2020/01/01/20200101_file.zlib.orc"

    fixture.writeBronze(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))
  }

  test("BronzeFileWriter#write write parquet dataframe compressed in file") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "sourceName1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("parquet"), filePattern = "<yyyy><mm><dd>_file.parquet", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "20200101_file.parquet"
    val expectedPath = testResourcesBasePath + "bronze/sourceName1/dataset1/2020/01/01/20200101_file.gz.parquet"

    fixture.writeBronze(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))
  }

  test("BronzeFileWriter#write write avro dataframe compressed in file") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "sourceName1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("avro"), filePattern = "<yyyy><mm><dd>_file.avro", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "20200101_file.avro"
    val expectedPath = testResourcesBasePath + "bronze/sourceName1/dataset1/2020/01/01/20200101_file.bz2.avro"

    fixture.writeBronze(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))
  }

  test("BronzeFileWriter#write write xls dataframe compressed in file") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset2", "", "sourceName1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("xls"), filePattern = "<yyyy><mm><dd>_file.xls", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "20200101_file.xls"
    val expectedPath = testResourcesBasePath + "bronze/sourceName1/dataset2/2020/01/01/20200101_file.csv.bz2"

    fixture.writeBronze(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))
  }
}
