package com.minsait.indation.metadata

import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models.{DatabricksProperties, DatabricksSecretsProperties, DatalakeProperties, IndationProperties, LandingProperties, MetadataProperties, SecurityEncryptionProperties, SecurityIdentityProperties, SecurityProperties}
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.minsait.indation.metadata.exceptions.SourceException

import java.io.File

class MetadataErrorSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"
  val ymlPath: String = testResourcesBasePath + "indation-test-config.yml"
  val metadataPath: String = testResourcesBasePath + "metadataerror"
  private val tmpDirectory = testResourcesBasePath + "tmp/"

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)

  }

  test("MetdataFilesManager#validateSources should detect wrong sources and datasets") {
    val metadataManager = new MetadataFilesManager(indationProperties)
    val path = "src/test/resources/metadataerror"
    val sources = metadataManager.validateMetadata(path)
    assert(sources._1.length == 1)
    assert(sources._2.length == 4)
  }
  test("Main#Main should throw SourceException") {
    val arguments = Array(
      "--config-file", ymlPath,
      "--validate-json", metadataPath)

    // Execution
    assertThrows[SourceException] {
      com.minsait.indation.Main.main(arguments)
    }
  }

  test("MetdataFilesManager#readDatasets with validation Datasets"){
    val metadataManager = new MetadataFilesManager(indationProperties)
    val datasets = metadataManager.readDatasets

    assert(datasets.length==100)
  }

  test("MetdataFilesManager#readSources with validation Sources"){
    val metadataManager = new MetadataFilesManager(indationProperties)
    val sources = metadataManager.readSources
    assert(sources.length==19)


  }
}
