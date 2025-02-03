package com.minsait.indation.datalake

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Dataset, FileInput, JsonFormat, SchemaColumns}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

class LandingFileManagerSpec extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper {

  import spark.implicits._

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

  test("LandingFileManager#absoluteFilePath return absolutePath of file") {
    assert(fixture.absoluteFilePath("landing/pending/source1/empty.csv") == testResourcesBasePath + "landing/" + "pending/source1/empty.csv")
  }

  test("LandingFileManager#deleteFile delete file from file system") {
    val filePath = testResourcesBasePath + "landing/pending/source1/delete.csv"
    fixture.deleteFile(filePath)
    assert(Files.notExists(Paths.get(filePath)))
  }

  test("LandingFileManager#copyToCorrupted copy file to corrupted") {
    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val versionFile = testResourcesBasePath + "landing/pending/source1/20200101_file.csv"

    fixture.copyToCorrupted(versionFile, testDataset)

    assert(Files.exists(Paths.get(testResourcesBasePath + "landing/corrupted/source1/dataset1/2020/01/01/20200101_file.csv")))
  }

  test("LandingFileManager#copyToUnknown copy file to unknown") {
    val filePath = testResourcesBasePath + "landing/pending/source1/unknown.csv"
    fixture.copyToUnknown(filePath)
    assert(Files.exists(Paths.get(testResourcesBasePath + "landing/unknown/source1/unknown.csv")))
  }

  test("LandingFileManager#copyToInvalid copy file to invalid") {
    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = Some(JsonFormat("UTF-8", true)), xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val versionFile = testResourcesBasePath + "landing/pending/source1/20200101_file.csv"

    fixture.copyToInvalid(versionFile, testDataset)

    assert(Files.exists(Paths.get(testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.csv")))
  }

  test("LandingFileManager#copyToSchemaMismatch copy file to schema-mismatch") {
    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val filePath = testResourcesBasePath + "landing/pending/source1/20200102_file.csv"

    fixture.copyToSchemaMismatch(filePath, testDataset)

    assert(Files.exists(Paths.get(testResourcesBasePath + "landing/schema-mismatch/source1/dataset1/2020/01/02/20200102_file.csv")))
  }

  test("LandingFileManager#copyToInvalid copy second file to invalid and create a new version file") {
    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val versionFile = testResourcesBasePath + "landing/pending/source1/20200101_file.csv"

    fixture.copyToInvalid(versionFile, testDataset)

    assert(Files.exists(Paths.get(testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.csv")))
  }

  test("LandingFileManager#writeToSchemaMismatch write file in csv extension in schema-missmatch path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("json"), filePattern = "<yyyy><mm><dd>_file.json", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)


    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.json"
    val expectedPath = testResourcesBasePath + "landing/schema-mismatch/source1/dataset1/2020/01/01/20200101_file.csv.bz2"

    fixture.writeToSchemaMismatch(source, initialDF, testDataset)
    fixture.writeToSchemaMismatch(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.format("csv").load(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in csv extension in invalid path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("csv"), filePattern = "<yyyy><mm><dd>_file.csv", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.csv"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.csv.bz2"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.csv(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in json extension in invalid path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("json"), filePattern = "<yyyy><mm><dd>_file.json", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.json"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.json.bz2"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.json(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in txt extension in invalid path") {

    val initialDF = Seq(
      "A|2|yyyy"
    ).toDF("value")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("text"), filePattern = "<yyyy><mm><dd>_file.txt", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.txt"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.txt.bz2"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.text(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in orc extension in invalid path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("orc"), filePattern = "<yyyy><mm><dd>_file.orc", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.orc"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.zlib.orc"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.orc(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in parquet extension in invalid path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("parquet"), filePattern = "<yyyy><mm><dd>_file.parquet", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.parquet"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.gz.parquet"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.option("spark.sql.parquet.int96RebaseModeInRead", "LEGACY").option("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY").parquet(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in avro extension in invalid path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset1", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("avro"), filePattern = "<yyyy><mm><dd>_file.avro", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.avro"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset1/2020/01/01/20200101_file.bz2.avro"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.format("avro").load(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }

  test("LandingFileManager#writeToInvalid write file in xls extension in invalid path") {

    val initialDF = Seq(
      ("A", "2020", "01", "01")
    ).toDF("_c0", "_c1", "_c2", "_c3")

    val testDataset = Dataset(Option(1), Option(1), Option(1)
      , "dataset2", "", "source1"
      , DatasetTypes.File
      , Some(FileInput(FileFormatType("xls"), filePattern = "<yyyy><mm><dd>_file.xls", fixed = None
        , csv = None, json = None, xls = None)), None, None, None
      , 1, enabled = false, Option(Timestamp.valueOf("2020-01-01 01:00:00")), createDatabase = false
      , ClassificationType("public"), "", Some(true), IngestionTypes.FullSnapshot
      , "", "", ValidationTypes.FailFast
      , 2, PermissiveThresholdTypes.Absolute
      , SchemaDefinitionType(""), Some(SchemaColumns(List())), None, None)

    val source = testResourcesBasePath + "landing/pending/source1/20200101_file.csv"
    val expectedPath = testResourcesBasePath + "landing/invalid/source1/dataset2/2020/01/01/20200101_file.csv.bz2"

    fixture.writeToInvalid(source, initialDF, testDataset)
    fixture.writeToInvalid(source, initialDF, testDataset)

    assert(Files.exists(Paths.get(expectedPath)))

    val finalData = spark.read.csv(expectedPath)

    assertSmallDatasetEquality(initialDF, finalData, orderedComparison = false)
  }
}
