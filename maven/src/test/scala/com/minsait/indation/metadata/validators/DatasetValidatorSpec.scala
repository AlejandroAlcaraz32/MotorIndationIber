package com.minsait.indation.metadata.validators

import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.SparkSessionFactory
import com.minsait.common.utils.EncryptUtils
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metadata.exceptions.DatasetException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.File

class DatasetValidatorSpec extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\", DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("", testResourcesBasePath, "", "")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("", testResourcesBasePath, "")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties(SecurityEncryptionTypes.Pseudonymization,
      Some("mLtzjLyHZaUzBN34hYuSwSWuK/Drbbw+EuZeFnIzoeA="),
      Some(true), Some(EncryptionAlgorithm(EncryptionAlgorithmTypes.Aes, EncryptionModeTypes.Ctr, "NoPadding")), Some("GXCQ9QgX4QZ4i7K")))
    , Local,
    tmpDirectory
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
    EncryptUtils.setProperties(indationProperties)
  }

  test("DatasetValidatorSpec# dataset KO"){
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("full-snapshot-worldcities-sensitive-validator")

    assertThrows[DatasetException] {
      DatasetValidator.validateDatasetColumns(dataset.get,indationProperties)
    }
  }

  test("DatasetValidatorSpec# dataset OK"){
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("full-snapshot-worldcities-sensitive-validator-2")

    try {
      DatasetValidator.validateDatasetColumns(dataset.get,indationProperties)
      assert(true)
    } catch {
      case _: Exception => assert(false)
    }
  }

  test("DatasetValidatorSpec# dataset onlyNewData KO") {
    val metadataReader = new MetadataFilesManager(indationProperties)
    val dataset = metadataReader.datasetByName("full-snapshot-worldcities-sensitive-validator-3")

    assertThrows[DatasetException] {
      DatasetValidator.validateDatasetColumns(dataset.get, indationProperties)
    }
  }
}
