package com.minsait.indation.silver

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.datalake.LandingFileManager
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType
import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums._
import com.minsait.indation.silver.transformers.SilverJsonColumnsSchemaTransformer
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.Timestamp

class SilverColumnTransformerSpec extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper{

  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\","/") + "/"
  class Fixture (val indationProperties: IndationProperties) extends ConfigurationReader with LandingFileManager

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  lazy val fixture = new Fixture(IndationProperties(
    MetadataProperties("","","",""),
    Some(DatabricksProperties(DatabricksSecretsProperties(""))),
    LandingProperties("AccountName1", testResourcesBasePath + "landing/", "landing", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming"),
    DatalakeProperties("","",""),
    SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties()),
    Local,
    tmpDirectory
  ))

  override def beforeAll() {
    SparkSessionFactory.configSparkSession(fixture.indationProperties)
  }

  test("SilverColumnTransformerSpec#transform correct dataframe transformation without invalid rows"){

    import spark.implicits._

    val testDataset  = Dataset(Option(1),Option(1),Option(1)
      ,"dataset1","","source1"
      ,DatasetTypes.File
      ,Some(FileInput(FileFormatType(""),filePattern = "",fixed = None
        ,csv = None, json= None, xls = None)), None, None, None
      ,1, enabled = false ,Option(Timestamp.valueOf("2020-01-01 01:00:00")) ,createDatabase = false
      ,ClassificationType("public") ,"", Some(true) ,IngestionTypes.FullSnapshot
      ,"","", ValidationTypes.FailFast
      ,2 , PermissiveThresholdTypes.Absolute
      ,SchemaDefinitionType(""), Some(SchemaColumns(List(
        Column("String",ColumnsType("string"),None,None,None,None,None,None,None,sensitive = false),
        Column("Integer",ColumnsType("integer"),None,None,None,None,None,None,None,sensitive = false),
        Column("Float",ColumnsType("float"),None,None,None,None,None,None,None,sensitive = false),
        Column("Double",ColumnsType("double"),None,None,None,None,None,None,None,sensitive = false),
        Column("Boolean",ColumnsType("boolean"),None,None,None,None,None,None,None,sensitive = false),
        Column("Date",ColumnsType("date"),None,None,None,None,None,None,None,sensitive = false),
        Column("Long",ColumnsType("long"),None,None,None,None,None,None,None,sensitive = false),
        Column("DateTime",ColumnsType("datetime"),None,None,None,None,None,None,None,sensitive = false)
      ))), None,None)

    val initialDF = Seq(
      ("A", "1", "1.1", "1.12345", "true", "01-01-2020", "2", "01-01-2020 23:59:59")
    ).toDF("String", "Integer", "Float", "Double", "Boolean", "Date", "Long", "DateTime")

    val expectedSchema = StructType(
      List(
        StructField("String", StringType, nullable = true),
        StructField("Integer", IntegerType, nullable = true),
        StructField("Float", FloatType, nullable = true),
        StructField("Double", DoubleType, nullable = true),
        StructField("Boolean", BooleanType, nullable = true),
        StructField("Date", DateType, nullable = true),
        StructField("Long", LongType, nullable = true),
        StructField("DateTime", TimestampType, nullable = true)
      )
    )

    val (validDf, invalidDf) = SilverJsonColumnsSchemaTransformer.transform(initialDF, testDataset)

    assert(validDf.schema == expectedSchema)
    assert(invalidDf.isEmpty)
  }

  test("SilverColumnTransformerSpec#transform correct dataframe transformation with invalid rows"){

    import spark.implicits._

    val testDataset  = Dataset(Option(1),Option(1),Option(1)
      ,"dataset1","","source1"
      ,DatasetTypes.File
      ,Some(FileInput(FileFormatType(""),filePattern = "",fixed = None
        ,csv = None, json= None, xls = None)), None, None, None
      ,1, enabled = false ,Option(Timestamp.valueOf("2020-01-01 01:00:00")) ,createDatabase = false
      ,ClassificationType("public") ,"", Some(true) ,IngestionTypes.FullSnapshot
      ,"","", ValidationTypes.FailFast
      ,2 , PermissiveThresholdTypes.Absolute
      ,SchemaDefinitionType(""), Some(SchemaColumns(List(
        Column("String",ColumnsType("string"),None,None,None,None,None,None,None,sensitive = false),
        Column("Integer",ColumnsType("integer"),None,None,None,None,None,None,None,sensitive = false),
        Column("Float",ColumnsType("float"),None,None,None,None,None,None,None,sensitive = false),
        Column("Double",ColumnsType("double"),None,None,None,None,None,None,None,sensitive = false),
        Column("Boolean",ColumnsType("boolean"),None,None,None,None,None,None,None,sensitive = false),
        Column("Date",ColumnsType("date"),None,None,None,None,None,None,None,sensitive = false),
        Column("Boolean2",ColumnsType("long"),None,None,None,None,None,None,None,sensitive = false)
      ))), None,None)

    val initialDF = Seq(
      ("A", "1", "1.1", "1.12345", "true", "01-01-2020", "NoBooleanValue")
    ).toDF("String", "Integer", "Float", "Double", "Boolean", "Date", "Boolean2")

    val (_, invalidDf) = SilverJsonColumnsSchemaTransformer.transform(initialDF, testDataset)

    assert(!invalidDf.isEmpty && invalidDf.select("Boolean2").head().isNullAt(0))
  }

  test("SilverColumnTransformerSpec#comma-eng"){

    import spark.implicits._

    val testDataset  = Dataset(Option(1),Option(1),Option(1)
      ,"dataset1","","source1"
      ,DatasetTypes.File
      ,Some(FileInput(FileFormatType(""),filePattern = "",fixed = None
        ,csv = None, json= None, xls = None)), None, None, None
      ,1, enabled = false ,Option(Timestamp.valueOf("2020-01-01 01:00:00")) ,createDatabase = false
      ,ClassificationType("public") ,"", Some(true) ,IngestionTypes.FullSnapshot
      ,"","", ValidationTypes.FailFast
      ,2 , PermissiveThresholdTypes.Absolute
      ,SchemaDefinitionType(""), Some(SchemaColumns(List(
        Column("Integer",ColumnsType("integer"),None,None,None,None,None,Some(ColumnTransformation(ColumnTransformationTypes.CommaEng, None, None)),None,
          sensitive =
          false),
        Column("Float",ColumnsType("float"),None,None,None,None,None,Some(ColumnTransformation(ColumnTransformationTypes.CommaEng, None, None)),None,
          sensitive = false),
        Column("Double",ColumnsType("double"),None,None,None,None,None,Some(ColumnTransformation(ColumnTransformationTypes.CommaEng, None, None)),None,
          sensitive = false),
        Column("Long",ColumnsType("long"),None,None,None,None,None,Some(ColumnTransformation(ColumnTransformationTypes.CommaEng, None, None)),None,sensitive =
          false)
      ))), None,None)

    val initialDF = Seq(
      ("1,450", "1,234.12", "1,234,567.89", "12,123,45,124")
    ).toDF("Integer", "Float", "Double", "Long")

    val expectedSchema = List(
        StructField("Integer", IntegerType, nullable = true),
        StructField("Float", FloatType, nullable = true),
        StructField("Double", DoubleType, nullable = true),
        StructField("Long", LongType, nullable = true)
    )

    val expectedDF = spark.createDF(
      List(
        (1450, 1234.12F, 1234567.89D, 1212345124L)
      ),
      expectedSchema
    )

    val (validDf, invalidDf) = SilverJsonColumnsSchemaTransformer.transform(initialDF, testDataset)

    assertSmallDatasetEquality(validDf, expectedDF, orderedComparison = false)
    assert(invalidDf.isEmpty)
  }

}