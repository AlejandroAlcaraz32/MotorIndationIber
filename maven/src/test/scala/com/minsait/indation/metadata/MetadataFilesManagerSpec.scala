package com.minsait.indation.metadata

import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.metadata.exceptions.NonUniqueDatasetException
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.SourceTypes
import com.minsait.indation.metadata.models.enums.ValidationTypes.FailFast
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class MetadataFilesManagerSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

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

  test("MetadataFilesManagerSpec#datasets") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val datasets: List[Dataset] = metadataManager.datasets

    assert(datasets.nonEmpty)
  }

  test("MetadataFilesManagerSpec#sources") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val sources = metadataManager.sources

    assert(sources.nonEmpty)
    assert(17 == sources.length)
  }

  test("MetadataFilesManagerSpec#datasetForFile non existing") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    assert(metadataManager.datasetForFile("20200704_dataset5.csv").isEmpty)
  }

  test("MetadataFilesManagerSpec#datasetForFile existing") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    assert(metadataManager.datasetForFile("20200717_atributos.csv").isDefined)
    assert(metadataManager.datasetForFile("20200717_atributos.csv").get.name == "atributos")
  }

  test("MetadataFilesManagerSpec#datasetForFile get newer version") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetForFile("20200717_atributos.csv")
    assert(dataset.isDefined)
    assert(dataset.get.validationMode == FailFast)
  }

  test("MetadataFilesManagerSpec#datasetForFile non unique") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    assertThrows[NonUniqueDatasetException] {
      metadataManager.datasetForFile("20200717_duplicado.csv")
    }
  }

  test("MetadataFilesManagerSpec#datasetForFile disabled") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    assert(metadataManager.datasetForFile("20200717_dataset3.avro").isEmpty)
  }

  test("MetadataFilesManagerSpec#datasetForFile enabled future") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    assert(metadataManager.datasetForFile("20200717_dataset6.txt").isEmpty)
  }

  test("MetadataFilesManagerSpec#sourceByName") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val source = Source(None, "source1", "Source 1", SourceTypes.Directory, None, None, List())
    assertResult(source.description)(metadataManager.sourceByName("source1").get.description)
  }

  //TODO: Corregir el test MetadataFilesManagerSpec#datasetByName o evaluar por qué no funciona
//  test("MetadataFilesManagerSpec#datasetByName") {
//    val metadataManager = new MetadataFilesManager(indationProperties)
//
//    val kafkaInput = KafkaInput("topicSecret", "dummyGroupId", Some(TopicOffsetTypes.Earliest), "keyVaultSecret", Some("https://server/dataset"))
//    val dataset = metadataManager.datasetByName("topic-worldcities").get
//    assertResult(kafkaInput)(dataset.kafkaInput.get)
//  }

  test("MetadataFilesManagerSpec#datasetsBySourceName") {
    val metadataManager = new MetadataFilesManager(indationProperties)
    val source = metadataManager.sourceByName("sourceMultiVersion")
    assert(source.isDefined)
    val datasets = metadataManager.datasetBySource(source.get)
    assert(datasets.length==2)
    val dsVersioned = datasets.filter(ds => ds.name == "full-snapshot-worldcities-orc-sourceMultiVersion")
    assert(dsVersioned.length==1 && dsVersioned(0).version==3)
  }

  test("MetadataFilesManagerSpec#datasetByNameJdbc_versionOk") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetByName("dataset-sqlserver-fullsnapshot-url")

    assert(dataset.isDefined)
    assertResult("dataset sqlserver full snapshot")(dataset.get.description)

  }

  test("MetadataFilesManagerSpec#datasetBySourceJdbc_versionOk") {
    val metadataManager = new MetadataFilesManager(indationProperties)

    val dataset = metadataManager.datasetByName("dataset-sqlserver-fullsnapshot-url")
    assert(dataset.isDefined)

    val source = metadataManager.sourceByName(dataset.get.sourceName)
    assert(source.isDefined)

    val datasets = metadataManager.datasetBySource(source.get)
    assert (datasets.length==2)

    val dataset2 = datasets.filter(p => p.name=="dataset-sqlserver-fullsnapshot-url")
    assert(dataset2.length==1)
    assertResult(dataset.get)(dataset2(0))

  }

 /* test ("MetadataFilesManagerSpec#ValidateSources"){
    val metadataManager = new MetadataFilesManager(indationProperties)

    val outputvalues = metadataManager.validateSources

    assert (outputvalues.length==1)

  }*/



}
