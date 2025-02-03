package com.minsait.indation.silver.helper

import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.metadata.MetadataFilesManager
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructType}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class SchemaHelperSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  val testResourcesBasePath: String = new File(this.getClass.getResource("/").getPath)
                                        .toString.replace("\\","/") + "/"
  private val tmpDirectory = testResourcesBasePath + "tmp/"

  val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("",testResourcesBasePath + "metadata","","")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("","","","","","","","","")
    , DatalakeProperties("","","")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  override def beforeAll() {
    SparkSessionFactory.configSparkSession(indationProperties)
  }

  test("SchemaHelper#primary keys json-schema") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200101_last_changes_worldCities.json").get
    val primaryKeys = SchemaHelper.primaryKeys(dataset)
    assertResult(List("geonameid"))(primaryKeys)
  }

  test("SchemaHelper#timeStamp column json-schema") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200101_last_changes_worldCities.json").get
    val primaryKeys = SchemaHelper.timeStampColumn(dataset)
    assertResult("timeStamp")(primaryKeys)
  }

  test("SchemaHelper#empty primary keys json-schema") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200101_worldCities.json").get
    val primaryKeys = SchemaHelper.primaryKeys(dataset)
    assertResult(List())(primaryKeys)
  }

  test("SchemaHelper#empty timeStamp column json-schema") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200101_worldCities.json").get
    val primaryKeys = SchemaHelper.timeStampColumn(dataset)
    assertResult("")(primaryKeys)
  }

  test("SchemaHelper#primary keys json-column") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200101_worldCities.csv").get
    val primaryKeys = SchemaHelper.primaryKeys(dataset)
    assertResult(List("geonameid"))(primaryKeys)
  }

  test("SchemaHelper#timeStamp column json-column") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200101_worldCities.csv").get
    val primaryKeys = SchemaHelper.timeStampColumn(dataset)
    assertResult("timeStamp")(primaryKeys)
  }
  test("SchemaHelper#persistence and alias columns json-schema") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile( "20200108_persistence_alias_columns_worldCities.json").get
    val persistentColumns = SchemaHelper.persistentColumns(dataset)

    val expectedResult: Map[String, Option[String]] = Map(
      "name" -> None,
      "country" -> Some("countryAlias"),
      "geonameid" -> None,
      "timeStamp" -> Some("timeStampAlias")
    )

    assertResult(expectedResult)(persistentColumns)
  }

  test("SchemaHelper# json-columns simple type columns") {
    val metadataReader = new MetadataFilesManager(indationProperties)

    val dataset = metadataReader.datasetByName("dataset-api-json-columns-1")
    val schema = SchemaHelper.createStructSchema(dataset.get.schemaColumns.get.columns)

    val schema_compare = new StructType()
      .add("UserName", StringType, true)
      .add("Concurrency", LongType, true)
      .add("FirstName", DoubleType, true)
      .add("LastName", StringType, true)

    assert(schema == schema_compare)
  }

  test("SchemaHelper# json-columns array struct columns") {
    val metadataReader = new MetadataFilesManager(indationProperties)

    val dataset = metadataReader.datasetByName("dataset-api-json-columns-2")
    val schema = SchemaHelper.createStructSchema(dataset.get.schemaColumns.get.columns)

    val schema_compare = new StructType()
      .add("UserName", StringType, true)
      .add("Concurrency", LongType, true)
      .add("FirstName", StringType, true)
      .add("LastName", StringType, true)
      .add("Gender", StringType, true)
      .add("Emails", ArrayType(StringType), true)
      .add("AddressInfo", ArrayType(new StructType()
        .add("Country", StringType, true)
        .add("Region", StringType, true)
        .add("Name", StringType, true)), true)

    assert(schema == schema_compare)
  }

  test("SchemaHelper# json-columns array struct columns 2") {
    val metadataReader = new MetadataFilesManager(indationProperties)

    val dataset = metadataReader.datasetByName("dataset-api-json-columns-3")
    val schema = SchemaHelper.createStructSchema(dataset.get.schemaColumns.get.columns)
    schema.printTreeString()

    val schema_compare = new StructType()
      .add("UserName", StringType, true)
      .add("Concurrency", LongType, true)
      .add("FirstName", StringType, true)
      .add("LastName", StringType, true)
      .add("Gender", StringType, true)
      .add("Emails", ArrayType(StringType), true)
      .add("AddressInfo", new StructType()
        .add("Address", StringType, true)
        .add("City", ArrayType(StringType), true))

    assert(schema == schema_compare)
  }

  test("SchemaHelper# json-schema array struct columns") {
    val metadataReader = new MetadataFilesManager(indationProperties)

    val dataset = metadataReader.datasetByName("dataset-api-json-schema")
    val schema = SchemaHelper.structTypeSchema(dataset.get)

    val schema_compare = new StructType()
      .add("UserName", StringType, true)
      .add("Concurrency", LongType, true)
      .add("FirstName", StringType, true)
      .add("LastName", StringType, true)
      .add("Gender", StringType, true)
      .add("Emails", ArrayType(StringType), true)
      .add("AddressInfo", ArrayType(new StructType()
        .add("Address", StringType, true)
        .add("City", new StructType()
          .add("Country", StringType, true)
          .add("Region", StringType, true)
          .add("Name", StringType, true))), true)

    assert(schema == schema_compare)
  }
}
