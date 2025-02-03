package com.minsait.indation.datalake

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.datalake.exceptions.{CreateDatabaseException, ReadDatalakeException}
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ClassificationTypes.Public
import com.minsait.indation.metadata.models.enums.IngestionTypes.LastChanges
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.Columns
import com.minsait.indation.metadata.models.enums.ValidationTypes.Permissive
import com.minsait.indation.metadata.models.enums._
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

class DatalakeManagerSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with DatasetComparer with SparkSessionWrapper {

  import spark.implicits._

  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"
  private var currentTimestamp: String = _
  private val silverHistoricalSuffix = "_historical"

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val indationProperties = IndationProperties(
    MetadataProperties("", "", "", "")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("", "", "", "", "", "", "", "", "")
    , DatalakeProperties("", "", "")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , Local,
    tmpDirectory
  )

  override def beforeAll() {
    currentTimestamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)
    SparkSessionFactory.configSparkSession(indationProperties)
  }

  private def copyFolder(source: File, destination: File): Unit = {
    if (source.isDirectory) {
      if (!destination.exists()) {
        destination.mkdirs()
      }

      val files = source.list()
      for (file <- files) {
        val srcFile = new File(source, file)
        val destFile = new File(destination, file)
        copyFolder(srcFile, destFile)
      }
    }
    else {
      Files.copy(Paths.get(source.getPath), Paths.get(destination.getPath), StandardCopyOption.REPLACE_EXISTING)
    }
  }

  test("DataLakeManager#getPendingFiles list pending files of a existing source should return non empty array.") {
    // Given
    val source = Source(Some(1), "source1", "", SourceTypes.Directory, None, None, List())

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val dataLakeManager = new DatalakeManager(configManager, currentTimestamp)
    val pendingFiles = dataLakeManager.getPendingFiles(source)

    // Then
    assert(pendingFiles.length > 0)
  }

  test("DataLakeManager#getPendingFiles list pending files of a non existing source should return empty array.") {
    // Given
    val source = Source(Some(1), "wrongSource", "", SourceTypes.Directory, None, None, List())

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val dataLakeManager = new DatalakeManager(configManager, currentTimestamp)
    val pendingFiles = dataLakeManager.getPendingFiles(source)

    // Then
    assert(pendingFiles.length == 0)
  }

  test("DataLakeManager#readPendingActualSchema read existing file should the file schema.") {
    // Given
    val filePath = testResourcesBasePath + "bronze/landing/pending/source1/prueba1.csv"
    val mockInputFormat = CsvFormat("US-ASCII", ",", CsvHeaderTypes.FirstLine, Some("\""), Some(false), None, None)
    val expectedSchema = StructType(
      List(
        StructField("ID", StringType, nullable = true),
        StructField("FIRST_NAME", StringType, nullable = true),
        StructField("LAST_NAME", StringType, nullable = true),
        StructField("AGE", StringType, nullable = true)
      )
    )

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")

    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val dataLakeManager = new DatalakeManager(configManager, currentTimestamp)
    val actualSchema = dataLakeManager.readPendingActualSchema(filePath, mockInputFormat)

    // Then
    assert(actualSchema == expectedSchema)
  }

  test("DataLakeManager#readPendingActualSchema read non existing file should return an empty schema.") {
    // Given
    val filePath = testResourcesBasePath + "bronze/landing/pending/source1/nonExistingFile"
    val mockInputFormat = CsvFormat("UTF-8", ",", CsvHeaderTypes.FirstLine, None, Some(false), None, None)

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")

    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val dataLakeManager = new DatalakeManager(configManager, currentTimestamp)
    val actualSchema = dataLakeManager.readPendingActualSchema(filePath, mockInputFormat)

    // Then
    assert(actualSchema.isEmpty)
  }

  test("DataLakeManager#readPending read existing file should return a non empty dataset.") {
    // Given
    val filePath = testResourcesBasePath + "bronze/landing/pending/source1/prueba1.csv"
    val mockInputFormat = CsvFormat("US-ASCII", ",", CsvHeaderTypes.FirstLine, Some("\""), Some(false), None, None)

    val mockFileInput = FileInput(FileFormatTypes.Csv, "FILE_<yyyy><mm><dd>.csv",
      Some(mockInputFormat), None, None, None)

    val mockSpecSchema = SchemaColumns(
      List(
        Column("ID", ColumnsTypes.String, None, Some("column1"), Some(true), Some(false), Some(false), None, None, sensitive = false),
        Column("FIRST_NAME", ColumnsTypes.String, None, Some("column2"), Some(true), Some(false), Some(false), None, None, sensitive = false),
        Column("LAST_NAME", ColumnsTypes.String, None, Some("column3"), Some(true), Some(false), Some(false), None, None, sensitive = false),
        Column("AGE", ColumnsTypes.String, None, Some("column4"), Some(true), Some(false), Some(false), None, None, sensitive = false)
      )
    )

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, Public,
      "yyyy/mm/dd", Some(true), LastChanges, "database", "tableName", Permissive, 10,
      PermissiveThresholdTypes.Absolute, Columns, Some(mockSpecSchema), None, None)
    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")

    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val dataLakeManager = new DatalakeManager(configManager, currentTimestamp)
    val contentDf = dataLakeManager.readPending(filePath, mockDataset)

    // Then
    assert(!contentDf.isEmpty)
  }

  test("DataLakeManager#readPending read non existing file should return an empty dataset.") {
    // Given
    val filePath = testResourcesBasePath + "bronze/landing/pending/source1/nonExistingFile"
    val mockInputFormat = CsvFormat("UTF-8", ",", CsvHeaderTypes.FirstLine, None, Some(false), None, None)

    val mockFileInput = FileInput(FileFormatTypes.Csv, "FILE_<yyyy><mm><dd>.csv",
      Some(mockInputFormat), None, None, None)

    val mockSpecSchema = SchemaColumns(
      List(
        Column("ID", ColumnsTypes.String, None, Some("column1"), Some(true), Some(false), Some(false), None, None, sensitive = false),
        Column("FIRST_NAME", ColumnsTypes.String, None, Some("column2"), Some(true), Some(false), Some(false), None, None, sensitive = false),
        Column("LAST_NAME", ColumnsTypes.String, None, Some("column3"), Some(true), Some(false), Some(false), None, None, sensitive = false),
        Column("AGE", ColumnsTypes.String, None, Some("column4"), Some(true), Some(false), Some(false), None, None, sensitive = false)
      )
    )

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, Public,
      "yyyy/mm/dd", Some(true), LastChanges, "database", "tableName", Permissive, 10,
      PermissiveThresholdTypes.Absolute, Columns, Some(mockSpecSchema), None, None)

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")

    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val dataLakeManager = new DatalakeManager(configManager, currentTimestamp)
    val contentDf = dataLakeManager.readPending(filePath, mockDataset)

    // Then
    assert(contentDf.isEmpty)
  }

  test("DataLakeManager#copyFileToError move existing file should move the file correctly.") {
    // Given
    val sourceName = "source1"
    val database = "indation_source1"
    val table = "table1"
    val fileName = "prueba1.gz"
    val fileNameVersion = "prueba1_v" + currentTimestamp + ".gz"
    val originalFilePath = testResourcesBasePath + "bronze/landing/pending/" + sourceName + "/" + fileName
    val testBasePath = testResourcesBasePath + "bronze/landing/error/"
    val expectedErrorPath = testBasePath + database + "/"  + table + "/" + fileName
    val expectedErrorPath2 = testBasePath + database + "/"  + table + "/" + fileNameVersion

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1",testResourcesBasePath, "container1", "bronze/", "silver/", "gold/", "versions/", "data/", "qa/error/")
    val landingProps = LandingProperties("AccountName1",testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming", "landing", "error", "archive")


    // When
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)
    datalakeManager.copyFileToError(new File(originalFilePath).toString, database, table, fileName)

    // Then
    assert(Files.exists(Paths.get(expectedErrorPath)))
    assert(Files.exists(Paths.get(new File(originalFilePath).toString)))

    datalakeManager.copyFileToError(new File(originalFilePath).toString, database, table, fileName)

    // Then
    assert(Files.exists(Paths.get(expectedErrorPath)))
    assert(Files.exists(Paths.get(expectedErrorPath2)))
    assert(Files.exists(Paths.get(new File(originalFilePath).toString)))

  }

  test("DataLakeManager#copyFileToError move non existing file should throw an error.") {
    // Given
    val database = "indation_source1"
    val table = "table1"
    val fileName = "wrongFile"
    val originalFilePath = testResourcesBasePath + "landing/pending/" + database + "/" + table + "/" + fileName

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1",testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1",testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming", "landing", "error", "archive")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)
    assert(Files.notExists(Paths.get(new File(originalFilePath).toString)))

    // When - Then
    assertThrows[ReadDatalakeException] {
      datalakeManager.copyFileToError(new File(originalFilePath).toString, database, table, fileName)
    }
  }

  test("DataLakeManager#moveFileToLandingArchive save file to archive should save the file correctly.") {
    // Given
    val sourceName = "source1"
    val database = "indation_source1"
    val table = "table1"
    val fileName = "prueba1.gz"
    val fileNameVersion = "prueba1_v" + currentTimestamp + ".gz"

    val filePath = testResourcesBasePath + "/bronze/landing/pending/"+ sourceName + "/" + fileName
    val testBasePath = testResourcesBasePath + "bronze/landing/archive/"

    val expectedSavePath = testBasePath + database + "/" + table + "/" + fileName
    val expectedSavePath2 = testBasePath + database + "/" + table + "/" + fileNameVersion

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1",testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1",testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming", "landing", "error", "archive")

    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)


    // When
    val realSavePath = datalakeManager.moveFileToLandingArchive(new File(filePath).toString, database, table, fileName)

    // Then
    assert(realSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(expectedSavePath)))
    assert(Files.notExists(Paths.get(new File(filePath).toString)))
    assert(!Files.isDirectory(Paths.get(expectedSavePath)))

    Files.copy(Paths.get(expectedSavePath), Paths.get(new File(filePath).toString), StandardCopyOption.REPLACE_EXISTING)
    datalakeManager.moveFileToLandingArchive(new File(filePath).toString, database, table, fileName)

    assert(Files.exists(Paths.get(expectedSavePath2)))
    assert(!Files.isDirectory(Paths.get(expectedSavePath2)))
    assert(Files.notExists(Paths.get(new File(filePath).toString)))

    // Restore file system
    Files.move(Paths.get(expectedSavePath), Paths.get(new File(filePath).toString), StandardCopyOption.REPLACE_EXISTING)
  }

  test("DataLakeManager#writeLandingError save file to error should save the file correctly. gz file") {
    // Given
    val sourceName = "source1"
    val database = "indation_source1"
    val table = "table1"
    val fileName = "prueba2.gz"
    val fileNameVersion = "prueba2_v" + currentTimestamp + ".gz"
    val inputFormat = CsvFormat("US-ASCII", ",", CsvHeaderTypes.FirstLine, Some("\""), Some(false), None, None)

    val filePath = testResourcesBasePath + "bronze/landing/pending/"+ sourceName + "/" + fileName
    val readDf = spark.read.text(filePath)
    val testBasePath = testResourcesBasePath + "bronze/landing/error/"
    val fileDate = "20200101_0000"

    val expectedSavePath = testBasePath + database + "/" + table + "/" + fileName
    val expectedSavePath2 = testBasePath + database + "/" + table + "/" + fileNameVersion

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1",testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1",testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming", "landing", "error", "archive")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When
    val realSavePath = datalakeManager.writeLandingError(readDf, database, table, inputFormat, fileName, fileDate)
    datalakeManager.writeLandingError(readDf, database, table, inputFormat, fileName, fileDate)

    // Then
    assert(realSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(expectedSavePath)))
    assert(!Files.isDirectory(Paths.get(expectedSavePath)))

    assert(Files.exists(Paths.get(expectedSavePath2)))
    assert(!Files.isDirectory(Paths.get(expectedSavePath2)))

  }

  test("DataLakeManager#writeLandingError save file to error should save the file correctly. csv file") {
    // Given
    val sourceName = "source1"
    val database = "indation_source1"
    val table = "table1"
    val fileName = "prueba1.csv"
    val fileNameVersion = "prueba1_v" + currentTimestamp + ".csv"
    val inputFormat = CsvFormat("US-ASCII", ",", CsvHeaderTypes.FirstLine, Some("\""), Some(false), None, None)

    val filePath = testResourcesBasePath + "bronze/landing/pending/"+ sourceName + "/" + fileName
    val readDf = spark.read.text(filePath)
    val testBasePath = testResourcesBasePath + "bronze/landing/error/"
    val fileDate = "20200101_0000"

    val expectedSavePath = testBasePath + database + "/" + table + "/" + fileName
    val expectedSavePath2 = testBasePath + database + "/" + table + "/" + fileNameVersion

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1",testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1",testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming", "landing", "error", "archive")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When
    val realSavePath = datalakeManager.writeLandingError(readDf, database, table, inputFormat, fileName, fileDate)
    datalakeManager.writeLandingError(readDf, database, table, inputFormat, fileName, fileDate)

    // Then


    assert(realSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(new File(expectedSavePath).toString)))
    assert(!Files.isDirectory(Paths.get(new File(expectedSavePath).toString)))

    assert(Files.exists(Paths.get(new File(expectedSavePath2).toString)))
    assert(!Files.isDirectory(Paths.get(new File(expectedSavePath2).toString)))

  }

  test("DataLakeManager#writeLandingError save file to error should save the file correctly. txt file") {
    // Given
    val sourceName = "source1"
    val database = "indation_source1"
    val table = "table1"
    val fileName = "prueba1.txt"
    val fileNameVersion = "prueba1_v" + currentTimestamp + ".txt"
    val inputFormat = CsvFormat("US-ASCII", ",", CsvHeaderTypes.FirstLine, Some("\""), Some(false), None, None)

    val filePath = testResourcesBasePath + "bronze/landing/pending/"+ sourceName + "/" + fileName
    val readDf = spark.read.text(filePath)
    val testBasePath = testResourcesBasePath + "bronze/landing/error/"
    val fileDate = "20200101_0000"

    val expectedSavePath = testBasePath + database + "/" + table + "/" + fileName
    val expectedSavePath2 = testBasePath + database + "/" + table + "/" + fileNameVersion

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1",testResourcesBasePath, "container1")
    val landingProps = LandingProperties("AccountName1",testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming", "landing", "error", "archive")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When
    val realSavePath = datalakeManager.writeLandingError(readDf, database, table, inputFormat, fileName, fileDate)
    datalakeManager.writeLandingError(readDf, database, table, inputFormat, fileName, fileDate)

    // Then


    assert(realSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(new File(expectedSavePath).toString)))
    assert(!Files.isDirectory(Paths.get(new File(expectedSavePath).toString)))

    assert(Files.exists(Paths.get(new File(expectedSavePath2).toString)))
    assert(!Files.isDirectory(Paths.get(new File(expectedSavePath2).toString)))
  }

  test("DataLakeManager#writeBronze save file to bronze with only an existing previous version should move to previous version and write the new one as _v1.") {
    // Given
    val database = "indation_source1"
    val table = "table1"
    val fileName = "file1.csv"
    val fileNameNew = "file1"
    val deltaFileName = "delta1.snappy.parquet"
    val readDf = spark.read.text(testResourcesBasePath + "bronze/landing/pending/source1/prueba1.csv")

    //		val baseTestPath = testResourcesBasePath  + "bronze/delta/" + database + "/"
    val baseTestPath = testResourcesBasePath + "bronze/data/" + database + "/" // Cambiado tras modificación de la ruta para uso de Parquet
    val baseDatasetPath = baseTestPath + table + "/" + currentTimestamp + "/"
    val deltaPathStr = baseDatasetPath + fileNameNew
    val deltaDirFile = new File(deltaPathStr)

    if (!deltaDirFile.exists()) {
      deltaDirFile.mkdirs()
    }
    val newFilePath = Files.createFile(Paths.get(new File(deltaPathStr).toString + "/" + deltaFileName))
    assert(Files.exists(newFilePath))

    val v1PathStr = baseDatasetPath + "versions/" + fileNameNew + "_v1"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When
    val realSavePath = datalakeManager.writeBronze(readDf, database, table, fileName, currentTimestamp)

    // Then
    assert(realSavePath == deltaPathStr)
    assert(Files.exists(Paths.get(new File(v1PathStr).toString)))

  }

  test("DataLakeManager#writeBronze save file to bronze with two existing previous version should move to previous version and write the new one as _v2.") {
    // Given
    val database = "indation_source1"
    val table = "table2"
    val fileName = "file1.csv"
    val fileNameNew = "file1"
    val deltaFileName = "delta2.snappy.parquet"
    val readDf = spark.read.text(testResourcesBasePath + "bronze/landing/pending/source1/prueba1.gz")

    // Create folder structure.
    // Current version
    //		val baseTestPath = testResourcesBasePath + "bronze/delta/" + database + "/"
    val baseTestPath = testResourcesBasePath + "bronze/data/" + database + "/" // Cambiado tras modificación de la ruta para uso de Parquet
    val baseDatasetPath = baseTestPath + table + "/" + currentTimestamp + "/"
    val deltaPathStr = baseDatasetPath + fileNameNew
    val deltaDirFile = new File(deltaPathStr)
    if (!deltaDirFile.exists()) {
      deltaDirFile.mkdirs()
    }
    val newFilePath = Files.createFile(Paths.get(deltaPathStr + "/" + deltaFileName))
    assert(Files.exists(newFilePath))

    // _v1
    val versionsPath = baseDatasetPath + "versions/" + fileNameNew + "_v"
    val v1PathStr = versionsPath + "1"
    val v1DirPath = new File(v1PathStr)
    if (!v1DirPath.exists()) {
      v1DirPath.mkdirs()
    }
    val v1FilePath = Files.createFile(Paths.get(v1PathStr + "/" + deltaFileName))
    assert(Files.exists(v1FilePath))

    val expectedV2Path = versionsPath + "2"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When
    val realSavePath = datalakeManager.writeBronze(readDf, database, table, fileName, currentTimestamp)

    // Then
    assert(realSavePath == deltaPathStr)
    assert(Files.exists(Paths.get(expectedV2Path)))

  }

  test("DataLakeManager#readBronze read existing file should return a non empty dataset.") {
    // Given
    //		val filePath = testResourcesBasePath + "bronze/delta/test_database/test_table/20200227_1538/file1/"
    val filePath = testResourcesBasePath + "bronze/data/test_database/test_table/20200227_1538/file1/" // Cambiado tras modificación de la ruta para uso de Parquet

    // When
    val dataLakeManager = new DatalakeManager
    val contentDf = dataLakeManager.readBronze(filePath)

    // Then
    assert(!contentDf.isEmpty)
  }

  test("DataLakeManager#readBronze read non existing file should return an empty dataset.") {
    // Given
    val filePath = "nonExistingFile"

    // When
    val dataLakeManager = new DatalakeManager
    val contentDf = dataLakeManager.readBronze(filePath)

    // Then
    assert(contentDf.isEmpty)
  }

  /**
   * deleteBrone cannot be tested because its purpose is to delete a DeltaTable and this cannot be achieved in local mode
   * because Spark is not configured and we do not have administrator permission to configure it in the local machine.
   * Therefore, this method can't only call deleteBronze, and catch its exceptions.
   */
  test("DataLakeManager#deleteBronze delete existing file should delete it.") {
    // Given
    //		val filePath = testResourcesBasePath + "bronze/delta/test_database/test_table/20200227_1538/file2/"
    val filePath = testResourcesBasePath + "bronze/data/test_database/test_table/20200227_1538/file2/" // Cambiado tras modificación de la ruta para uso de Parquet
    // Copy folder to keep the original data unmodified.
    //		val deltaPath = testResourcesBasePath + "bronze/delta/deleteBronzedatabase/deleteBronzeTable/20200227_1538/file2tmp"
    val deltaPath = testResourcesBasePath + "bronze/data/deleteBronzedatabase/deleteBronzeTable/20200227_1538/file2tmp" // Cambiado tras modificación de la ruta para uso de Parquet
    copyFolder(new File(filePath), new File(deltaPath))

    // Create mock data.
    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // Execute tested method.
    // When - Then
    datalakeManager.deleteBronzeData(deltaPath)

    assert(Files.notExists(Paths.get(deltaPath)))
  }

  test("DataLakeManager#writeSilver createDatabase false and non-existing datalake path" +
    " should throw an createDatabaseException.") {
    // Given
    val ingestionType = IngestionTypes.FullSnapshot
    val database = "wrongdatabase"
    val table = "table1"
    val partitionBy = ""
    val primaryKeys: List[String] = List()
    val createDatabase = false
    val classification = "public"

    val readDF = spark.emptyDataFrame

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When - Then
    assertThrows[CreateDatabaseException] {
      datalakeManager.writeSilver(ingestionType, database, table, readDF,
        partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)
    }
  }

  test("DataLakeManager#writeSilver createDatabase false, existing datalake path " +
    "but not existing database should throw an createDatabaseException.") {
    // Given
    val ingestionType = IngestionTypes.FullSnapshot
    val database = "indation_source1"
    val table = "table1"
    val partitionBy = ""
    val primaryKeys: List[String] = List()
    val createDatabase = false
    val classification = "public"

    val readDF = spark.emptyDataFrame

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    // When - Then
    assertThrows[CreateDatabaseException] {
      datalakeManager.writeSilver(ingestionType, database, table, readDF,
        partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)
    }
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration and FULL " +
    "ingestion mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.FullSnapshot
    val database = "databaseFull"
    val table = "tableFull"
    val partitionBy = "yyyy/mm/dd"
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val initialRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)

    assert(initialRealSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(initialRealSavePath)))

    val initialDelta = spark.read.format("delta").load(initialRealSavePath)
    val initialHistoricalDelta = spark.read.format("delta").load(initialRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(initialDelta, initialDF, orderedComparison = false)
    assertSmallDatasetEquality(initialHistoricalDelta, initialDelta, orderedComparison = false)

    /* Second writeSilver execution. */
    val newDF = Seq(
      ("A", "2020", "01", "02"),
      ("B", "2020", "01", "02"),
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      newDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)
    assert(newRealSavePath == initialRealSavePath)
    assert(Files.exists(Paths.get(newRealSavePath)))

    val newDelta = spark.read.format("delta").load(newRealSavePath)
    val newHistoricalDelta = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(newDelta, newDF, orderedComparison = false)
    assertSmallDatasetEquality(newHistoricalDelta, initialDF.union(newDF), orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration, FULL " +
    "ingestion mode and partial reprocessed mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.FullSnapshot
    val database = "databaseFull"
    val table = "tableFull"
    val partitionBy = "yyyy/mm/dd"
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)

    /* Second writeSilver execution. */
    val partialReprocessedDF = Seq(
      ("C", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      partialReprocessedDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.Partial, true)

    val deltaData = spark.read.format("delta").load(newRealSavePath)
    val historicalDeltaData = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    val unionDeltaDf = initialDF.union(partialReprocessedDF)

    assertSmallDatasetEquality(deltaData, unionDeltaDf, orderedComparison = false)
    assertSmallDatasetEquality(historicalDeltaData, unionDeltaDf, orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration, FULL " +
    "ingestion mode and full reprocessed mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.FullSnapshot
    val database = "databaseFull"
    val table = "tableFull"
    val partitionBy = "yyyy/mm/dd"
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A1", "2020", "01", "01"),
      ("B1", "2020", "01", "01"),
      ("C1", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)

    /* Second writeSilver execution. */
    val fullReprocessedDF = Seq(
      ("A2", "2020", "01", "01"),
      ("B2", "2020", "01", "01"),
      ("D2", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      fullReprocessedDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.Full, true)

    val deltaData = spark.read.format("delta").load(newRealSavePath)
    val historicalDeltaData = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(deltaData, fullReprocessedDF, orderedComparison = false)
    assertSmallDatasetEquality(historicalDeltaData, fullReprocessedDF, orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration and ONLY_NEW " +
    "ingestion mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.Incremental
    val database = "databaseOnlyNew"
    val table = "tableOnlyNew"
    val partitionBy = "yyyy/mm/dd"
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val initialRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)
    assert(initialRealSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(new File(initialRealSavePath).toString)))

    val initialDelta = spark.read.format("delta").load(initialRealSavePath)

    assertSmallDatasetEquality(initialDelta, initialDF, orderedComparison = false)

    /* Second writeSilver execution. */
    val newDF = Seq(
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      newDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)
    assert(newRealSavePath == initialRealSavePath)
    assert(Files.exists(Paths.get(newRealSavePath)))

    val newDelta = spark.read.format("delta").load(newRealSavePath)

    assertSmallDatasetEquality(newDelta, initialDF.union(newDF), orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration ONLY_NEW " +
    "ingestion mode and partial reprocess mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.Incremental
    val database = "databaseOnlyNew"
    val table = "tableOnlyNew"
    val partitionBy = "yyyy/mm/dd"
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)

    /* Second writeSilver execution. */
    val partialReprocessedDF = Seq(
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      partialReprocessedDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.Partial, true)

    val deltaData = spark.read.format("delta").load(newRealSavePath)

    assertSmallDatasetEquality(deltaData, initialDF.union(partialReprocessedDF), orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration ONLY_NEW " +
    "ingestion mode and full reprocess mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.Incremental
    val database = "databaseOnlyNew"
    val table = "tableOnlyNew"
    val partitionBy = "yyyy/mm/dd"
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.None, true)

    /* Second writeSilver execution. */
    val fullReprocessedDF = Seq(
      ("A1", "2020", "01", "01"),
      ("B2", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      fullReprocessedDF, partitionBy, primaryKeys, createDatabase, classification, "", ReprocessTypes.Full, true)

    val deltaData = spark.read.format("delta").load(newRealSavePath)

    val deltaTable = DeltaTable.forPath(newRealSavePath)
    val df = deltaTable.history(1)

    assertSmallDatasetEquality(deltaData, fullReprocessedDF, orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration and " +
    "NEW_AND_MODIFIED ingestion mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.LastChanges
    val database = "databaseNewAndModified"
    val table = "tableNewAndModifed"
    //		val partitionBy = "yyyy/mm/dd"
    val partitionBy = "yyyy/mm" //Quitamos dd de la partición porque es cambiante, queda pendiente revisar si es correcto
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val initialRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)
    assert(initialRealSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(initialRealSavePath)))

    val initialDelta = spark.read.format("delta").load(initialRealSavePath)
    val initialHistoricalDelta = spark.read.format("delta").load(initialRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(initialDelta, initialDF, orderedComparison = false)
    assertSmallDatasetEquality(initialHistoricalDelta, initialDelta, orderedComparison = false)

    /* Second writeSilver execution. */
    val newDF = Seq(
      ("B", "2020", "01", "02"),
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      newDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)
    assert(newRealSavePath == initialRealSavePath)
    assert(Files.exists(Paths.get(newRealSavePath)))

    val newDelta = spark.read.format("delta").load(newRealSavePath)
    val newHistoricalDelta = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    val expectedFinalDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "02"),
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    assertSmallDatasetEquality(newDelta, expectedFinalDF, orderedComparison = false)
    assertSmallDatasetEquality(newHistoricalDelta, initialDF.union(newDF), orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration in" +
    "NEW_AND_MODIFIED ingestion mode and full reprocessed mode should write historical table to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.LastChanges
    val database = "databaseNewAndModified"
    val table = "tableNewAndModifed"
    //		val partitionBy = "yyyy/mm/dd"
    val partitionBy = "yyyy/mm" //Quitamos dd de la partición porque es cambiante, queda pendiente revisar si es correcto
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)

    /* Second writeSilver execution. */
    val fullReprocessedDF = Seq(
      ("A1", "2020", "01", "01"),
      ("B2", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      fullReprocessedDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.Full, true)

    val newHistoricalDelta = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(newHistoricalDelta, fullReprocessedDF, orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  test("DataLakeManager#writeSilver correct createDatabase configuration in" +
    "NEW_AND_MODIFIED ingestion mode and partial reprocessed mode should write historical table to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.LastChanges
    val database = "databaseNewAndModified"
    val table = "tableNewAndModifed"
    //		val partitionBy = "yyyy/mm/dd"
    val partitionBy = "yyyy/mm" //Quitamos dd de la partición porque es cambiante, queda pendiente revisar si es correcto
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)

    /* Second writeSilver execution. */
    val partialReprocessedDF = Seq(
      ("C", "2020", "01", "01"),
      ("D", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      partialReprocessedDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.Partial, true)

    val newHistoricalDelta = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(newHistoricalDelta, initialDF.union(partialReprocessedDF), orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  /*
   * Test to check if NEW_AND_MODIFIED ingestion mode is updating correctly
   * the Delta table, performing the schema evolution.
   * So, writeSilver will be called twice: with initial data and with new data.
   */
  test("DataLakeManager#writeSilver correct createDatabase configuration and " +
    "NEW_AND_MODIFIED ingestion mode with new columns should write to silver " +
    "performing the schema evolution properly.") {
    // Common data.
    val ingestionType = IngestionTypes.LastChanges
    val database = "databaseNewAndModifiedEvo"
    val table = "tableNewAndModifedEvo"
    //		val partitionBy = "yyyy/mm/dd"
    val partitionBy = "yyyy/mm" //Quitamos dd de la partición porque es cambiante, queda pendiente revisar si es correcto
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "01")
    ).toDF("col1", "yyyy", "mm", "dd")

    val initialRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)
    assert(initialRealSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(initialRealSavePath)))

    val initialDelta = spark.read.format("delta").load(initialRealSavePath)
    val initialHistoricalDelta = spark.read.format("delta").load(initialRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(initialDelta, initialDF, orderedComparison = false)
    assertSmallDatasetEquality(initialHistoricalDelta, initialDelta, orderedComparison = false)

    /* Second writeSilver execution. */
    /*
     * Using mergeSchema, new columns are added to the end and assertSmallDatasetEquality needs columns in order.
     * So, for unit testing new columns must be added to the end in expectedFinalDF.
     * In a real scenario order should not be a problem.
     */
    val newDF = Seq(
      ("B", "2020", "01", "02", "col2Value1"),
      ("C", "2020", "01", "02", "col2Value2")
    ).toDF("col1", "yyyy", "mm", "dd", "col2")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      newDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)
    assert(newRealSavePath == initialRealSavePath)
    assert(Files.exists(Paths.get(newRealSavePath)))

    val newDelta = spark.read.format("delta").load(newRealSavePath)
    val newHistoricalDelta = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    // Using mergeSchema, new columns are added to the end and assertSmallDatasetEquality needs columns in order.
    // So, new columns must be added to the end in expectedFinalDF.
    val expectedFinalDF = Seq(
      ("A", "2020", "01", "01", null),
      ("B", "2020", "01", "02", "col2Value1"),
      ("C", "2020", "01", "02", "col2Value2")
    ).toDF("col1", "yyyy", "mm", "dd", "col2")

    val expectedFinalHistoricalDF = initialDF
      .withColumn("col2", lit(null))
      .union(newDF)

    assertSmallDatasetEquality(newDelta, expectedFinalDF, orderedComparison = false)
    assertSmallDatasetEquality(newHistoricalDelta, expectedFinalHistoricalDF, orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }

  test("DataLakeManager#writeSilver correct createTenant configuration and unordered " +
    "DELTA_FROM_NON_HISTORIZED_SNAPSHOT ingestion mode should write to silver properly.") {
    // Common data.
    val ingestionType = IngestionTypes.LastChanges
    val database = "databaseNewAndModified2"
    val table = "tableNewAndModifiedData"
    //		val partitionBy = "yyyy/mm/dd"
    val partitionBy = "yyyy/mm" //Quitamos dd de la partición porque es cambiante, queda pendiente revisar si es correcto
    val primaryKeys: List[String] = List("col1")
    val createDatabase = true
    val classification = "public"

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val environment = EnvironmentTypes.Local
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    when(configManager.getEnvironment).thenReturn(environment)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)

    val datalakeManager = new DatalakeManager(configManager, currentTimestamp)

    val testBasePath = testResourcesBasePath + "silver/" + classification + "/" + database
    val expectedSavePath = testBasePath + "/" + table

    /* First writeSilver execution. */
    val initialDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    val initialRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      initialDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)
    assert(initialRealSavePath == expectedSavePath)
    assert(Files.exists(Paths.get(initialRealSavePath)))

    val initialDelta = spark.read.format("delta").load(initialRealSavePath)
    val initialHistoricalDelta = spark.read.format("delta").load(initialRealSavePath + silverHistoricalSuffix)

    assertSmallDatasetEquality(initialDelta, initialDF, orderedComparison = false)
    assertSmallDatasetEquality(initialHistoricalDelta, initialDelta, orderedComparison = false)

    /* Second writeSilver execution. */
    val newDF = Seq(
      ("B", "2020", "01", "01"),
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    val newRealSavePath = datalakeManager.writeSilver(ingestionType, database, table,
      newDF, partitionBy, primaryKeys, createDatabase, classification, "dd", ReprocessTypes.None, true)
    assert(newRealSavePath == initialRealSavePath)
    assert(Files.exists(Paths.get(newRealSavePath)))

    val newDelta = spark.read.format("delta").load(newRealSavePath)
    val newHistoricalDelta = spark.read.format("delta").load(newRealSavePath + silverHistoricalSuffix)

    val expectedFinalDF = Seq(
      ("A", "2020", "01", "01"),
      ("B", "2020", "01", "02"),
      ("C", "2020", "01", "02")
    ).toDF("col1", "yyyy", "mm", "dd")

    assertSmallDatasetEquality(newDelta, expectedFinalDF, orderedComparison = false)
    assertSmallDatasetEquality(newHistoricalDelta, initialDF.union(newDF), orderedComparison = false)

    // Restore file system
    DeltaTable.forPath(expectedSavePath).delete()
    DeltaTable.forPath(expectedSavePath + silverHistoricalSuffix).delete()
  }
}

