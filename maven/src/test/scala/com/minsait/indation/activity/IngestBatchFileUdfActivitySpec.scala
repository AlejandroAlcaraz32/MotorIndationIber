package com.minsait.indation.activity

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Timestamp

class IngestBatchFileUdfActivitySpec extends AnyFunSuite with MockitoSugar  with BeforeAndAfterAll with BeforeAndAfterEach
  with DatasetComparer with SparkSessionWrapper {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\",DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("",testResourcesBasePath,"","")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("",testResourcesBasePath,"")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  private val dataLakeStorageDate = "datalake_load_date"
  private val dataLakeStorageDay = "datalake_load_day"
  private val dataLakeIngestionUuid = "datalake_ingestion_uuid"
  private val historicalSuffix = "_historical"
  private val adfRunId = "adf-uuid"

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
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
  test("IngestBatchFileUdfActivity#udf mask-asterisk") {
    val filePath = "pending/source-udf/20200101_mask_asterisk_worldCities.csv"
    val ingestBatchActivity = new IngestBatchFileActivity(indationProperties)
    ingestBatchActivity.executeWithFile(filePath, adfRunId)

    val filePath2 = "pending/source-udf/20200102_mask_asterisk_worldCities.csv"
    ingestBatchActivity.executeWithFile(filePath2, adfRunId)

    val silverDF = spark.sql("select * from database6.full_snapshot_mask_asterisk_worldcities")
    val silverDFHistorical = spark.sql("select * from database6.full_snapshot_mask_asterisk_worldcities" + historicalSuffix)

    assert(silverDF.count() == 2)
    assert(silverDFHistorical.count() == 4)
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath)))
    assert(Files.notExists(Paths.get(this.indationProperties.landing.basePath + filePath2)))

    val spain = "Spain"
    val silverSchema = List(
      ("name", StringType, true),
      ("country", StringType, true),
      ("subcountry", StringType, true),
      ("geonameid", LongType, true),
      ("timeStamp", TimestampType, true),
      ("yyyy", StringType, true),
      ("mm", StringType, true),
      ("dd", StringType, true)
    )

    val silverTable = spark.createDF(
      List(
        ("Alicante", spain, "****************", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02"),
        ("Cáceres", spain, "****************", 2520611L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDF.drop(dataLakeStorageDate).drop(dataLakeIngestionUuid).drop(dataLakeStorageDay), silverTable, orderedComparison = false)

    val silverHistoricalTable = spark.createDF(
      List(
        ("Alicante", spain, "****************", 2521978L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Cáceres", spain, "****************", 2520611L, Timestamp.valueOf("2020-01-01 01:00:00"), "2020", "01", "01"),
        ("Alicante", spain, "****************", 2521978L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02"),
        ("Cáceres", spain, "****************", 2520611L, Timestamp.valueOf("2020-01-02 01:00:00"), "2020", "01", "02")
      ),
      silverSchema
    )

    assertSmallDatasetEquality(silverDFHistorical.drop(dataLakeStorageDate).drop(dataLakeIngestionUuid).drop(dataLakeStorageDay),
      silverHistoricalTable, orderedComparison = false)
  }
}
