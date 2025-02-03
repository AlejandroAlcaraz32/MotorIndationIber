package com.minsait.indation.datalake.readers

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.datalake.LandingFileManager
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType
import com.minsait.indation.metadata.models.enums.CsvHeaderTypes.CsvHeaderType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.{DatasetTypes, IngestionTypes, PermissiveThresholdTypes, ValidationTypes}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.Timestamp

class LandingCsvFileReaderSpec extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper {

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

  test("Readers#LandingCsvFileReader read a csv file with header") {

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
        Column("geonameid", ColumnsType("integer"), None, None, None, None, None, None, None, sensitive = false)))), None, None)

    val filePath = testResourcesBasePath + "landing/pending/source1/world-cities.csv"

    val df = LandingCsvFileReader.readFile(filePath, testDataset)

    assert(df.isSuccess)
  }

}
