package com.minsait.indation.bronze

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ClassificationTypes.Public
import com.minsait.indation.metadata.models.enums.ColumnsTypes.Integer
import com.minsait.indation.metadata.models.enums.IngestionTypes.LastChanges
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.Columns
import com.minsait.indation.metadata.models.enums.ValidationTypes.Permissive
import com.minsait.indation.metadata.models.enums._
import com.minsait.indation.metalog.MetaInfo.Layers
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.metalog.models.{DatasetLog, Ingestion, SourceLog}
import com.minsait.indation.metalog.{MetaInfo, MetaLogManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

class BronzeEngineSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  import spark.implicits._

  private val sourceName = "source1"
  private val database = "database"
  private val tableName = "tableName"
  private val permissiveThreshold = 2

  private val col1Name = "column1"
  private val col2Name = "column2"
  private val corruptColName = "_corrupt_record"

  private val mockFileInput = FileInput(FileFormatTypes.Csv, "FILE_<yyyy><mm><dd>.csv",
    Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)), None, None, None)

  private val mockSpecSchema = SchemaColumns(
    List(
      Column(col1Name, Integer, Some("column1"), Some("column1"), Some(true), Some(false), Some(false), None, None, sensitive = false),
      Column(col2Name, ColumnsTypes.Integer, Some("column2"), Some("column2"), Some(true), Some(false), Some(false), None, None, sensitive = false)
    )
  )

  private val mockSchemaStruct = StructType(
    List(
      StructField(col1Name, StringType, nullable = true),
      StructField(col2Name, StringType, nullable = true)
    )
  )

  private val mockDataset = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
    Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, Public,
    "yyyy/mm/dd", Some(true), LastChanges, database, tableName, Permissive, permissiveThreshold,
    PermissiveThresholdTypes.Absolute, Columns, Some(mockSpecSchema), None, None)
  private val mockSource = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

  private val fileName1 = "FILE_20180314.csv"
  private val fileName2 = "FILE_20180314.csv.fix"
  private val mockFile1 = s"""/bronze/landing/pending/$sourceName/$fileName1"""
  private val mockFile2 = s"""/bronze/landing/pending/$sourceName/$fileName2"""

  private val mockIngestion = Ingestion()
  private val mockSourceLog = SourceLog()
  private val mockDatasetLog = DatasetLog(outputPath = Some("outputpath"))

  private val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"
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
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
  }

  test("·BronzeEngine#process not read nothing when there are not pending files.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array())

    when(metaLogManager.initIngestionLog(Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(mockSource.sourceId.get, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, never).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, never).readPending(anyString, any[Dataset])
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, never).initDatasetLog(anyInt, anyInt)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, never).finishDatasetLog(any[DatasetLog])
  }

  test("BronzeEngine#process finish when exists running execution") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array())

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(mockSource.sourceId.get, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(true)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, never).getPendingFiles(mockSource)
    verify(datalakeManager, never).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, never).readPending(anyString, any[Dataset])

    verify(metaLogManager, never).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, never).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, never).initDatasetLog(anyInt, anyInt)
    verify(metaLogManager, never).finishSourceLog(mockSourceLog)
    verify(metaLogManager, never).finishIngestionLog(mockIngestion)
    verify(metaLogManager, never).finishDatasetLog(any[DatasetLog])
  }

  test("BronzeEngine#process read file whose dataset file pattern not matches the expected one does nothing.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val fileName = "wrongDataset_12345678.csv"
    val wrongmockFile1 = s"""/bronze/landing/pending/$sourceName/$fileName"""

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(wrongmockFile1))

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, never).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, never).readPending(anyString, any[Dataset])
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, never).initDatasetLog(anyInt, anyInt)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, never).finishDatasetLog(any[DatasetLog])
  }

  test("BronzeEngine#process read file whose dataset file pattern matches but headers does not should move file to error.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val mockFile1Content = Seq(("84700326", "2", null), ("84700346", "2", null)).toDF(col1Name, col2Name, corruptColName)

    val wrongSchemaStruct = StructType(
      List(
        StructField("wrongCol", StringType, nullable = true),
        StructField(col2Name, StringType, nullable = true),
        StructField(corruptColName, StringType, nullable = true)
      )
    )

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(wrongSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).copyFileToError(mockFile1, database, tableName, fileName1)
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file in permissive mode with correct structure but more wrong files than threshold in absolute mode should move file to error.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val wrongmockFile1Content =
      Seq(
        ("84700326", "2", null), // Valid
        ("84700346", "2", "84700346|2|extraCol"), // Wrong
        ("84700346", null, "84700346"), // Wrong
        ("84700346", "extra_col", "84700346|extra_col|22") // Wrong
      ).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(wrongmockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).copyFileToError(mockFile1, database, tableName, fileName1)
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)


    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file in permissive mode with correct structure but more wrong files than threshold in percentage mode should move file to error.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val mockDatasetWithPercentage = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.Permissive, 20,
      PermissiveThresholdTypes.Percentage, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSourceWithPercentage = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDatasetWithPercentage))

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val wrongmockFile1Content =
      Seq(
        ("84700326", "2", null), // Valid
        ("84700346", "2", "84700346|2|extraCol"), // Wrong
        ("84700346", null, "84700346"), // Wrong
        ("84700346", "extra_col", "84700346|extra_col|22") // Wrong
      ).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSourceWithPercentage)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDatasetWithPercentage)).thenReturn(wrongmockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSourceWithPercentage.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSourceWithPercentage.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSourceWithPercentage)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSourceWithPercentage)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDatasetWithPercentage)
    verify(datalakeManager, times(1)).copyFileToError(mockFile1, database, tableName, fileName1)
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)


    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSourceWithPercentage.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSourceWithPercentage.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file in permissive with correct structure and less wrong rows than threshold should save data in bronze and error.") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val datalakeManager: DatalakeManager = mock[DatalakeManager]

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val mockFile1Content =
      Seq(
        ("84700326", "2", null), // Valid
        ("84700346", "2", null), // Valid
        ("84700346,2", null, "84700346,2"), // Wrong
        ("84700346", "extra_col", "84700346|extra_col|2") // Wrong
      ).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file in failfast mode with correct structure and less wrong files than threshold should move file to error.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val database = "database"
    val tableName = "tableName"

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val wrongmockFile1Content =
      Seq(
        ("84700326", "2", null), // Valid
        ("84700346", "2", "84700346|2|extraCol"), // Wrong
        ("84700347", "2", null), // Valid
        ("84700348", "2", null) // Valid
      ).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(wrongmockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).copyFileToError(mockFile1, database, tableName, fileName1)
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read .csv file with all rows correct should save data in bronze.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val mockFile1Content = Seq(("84700326", "2", null), ("84700346", "2", null)).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read partial reprocess type with all rows correct should save data in bronze.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val mockFile1Content = Seq(("84700326", "2", null), ("84700346", "2", null)).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile2))
    when(datalakeManager.readPendingActualSchema(mockFile2, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile2, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile2, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile2, mockDataset)
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)

    assert(mockDatasetLog.reprocessed.contains(ReprocessTypes.Partial))
  }

  test("BronzeEngine#process read total reprocess type with all rows correct should save data in bronze.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.FirstLine, None, Some(false), None, None)
    val mockFile1Content = Seq(("84700326", "2", null), ("84700346", "2", null)).toDF(col1Name, col2Name, corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.isFullReprocess(fileName1)).thenReturn(true)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)

    assert(mockDatasetLog.reprocessed.contains(ReprocessTypes.Full))
  }

  test("BronzeEngine#process read file without headers (so, dataframe can have any column names) and with all rows correct should save data in bronze.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val mockFileInput = FileInput(FileFormatTypes.Csv, "FILE_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.WithoutHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.WithoutHeader, None, Some(false), None, None)
    val mockFile1Content = Seq(("84700326", "2", null), ("84700346", "2", null)).toDF("col1", "col2", corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)

    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file with <yyyy><mm><dd>_fileName pattern.") {
    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val fileName = "20180314_FILE.csv"
    val mockFile1 = s"""/bronze/landing/pending/$sourceName/$fileName"""

    val mockFileInput = FileInput(FileFormatTypes.Csv, "<yyyy><mm><dd>_FILE.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.WithoutHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "table1", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.Permissive, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val mockInputFormat = CsvFormat("US-ASCII", "|", CsvHeaderTypes.WithoutHeader, None, Some(false), None, None)
    val mockFile1Content = Seq(("84700326", "2", null), ("84700346", "2", null)).toDF("col1", "col2", corruptColName)

    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)
    when(datalakeManager.getPendingFiles(mockSource)).thenReturn(Array(mockFile1))
    when(datalakeManager.readPendingActualSchema(mockFile1, mockInputFormat)).thenReturn(mockSchemaStruct)
    when(datalakeManager.readPending(mockFile1, mockDataset)).thenReturn(mockFile1Content)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(mockFile1, mockInputFormat)
    verify(datalakeManager, times(1)).readPending(mockFile1, mockDataset)
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file with ignore headers all rows correct should save data in bronze.") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba2_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba2", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source2", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager,
      new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read empty file must fail with message") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba3_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba3", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.FullSnapshot, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source3", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager, new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, times(1)).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read empty file (only header row) must fail with message") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba4_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba4", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.FullSnapshot, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source4", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager, new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, times(1)).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read empty file (only header row) ignore header must fail with message") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba5_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba5", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.FullSnapshot, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source5", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager, new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, never).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, times(1)).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file (header extra column) ignore header must finish ok") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba6_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba6", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source6", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager,
      new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file (header only one column) ignore header must finish ok") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba7_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba7", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source7", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager,
      new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }

  test("BronzeEngine#process read file (corrupted header) ignore header must finish ok") {
    // Given
    val metaLogManager: MetaLogManager = mock[MetaLogManager]

    val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\", "/") + "/"

    val mockFileInput = FileInput(FileFormatTypes.Csv, "prueba8_<yyyy><mm><dd>.csv", Some(CsvFormat("US-ASCII", "|", CsvHeaderTypes.IgnoreHeader, None,
      Some(false), None, None)), None, None, None)

    val mockDataset = Dataset(Some(1), Some(1), Some(1), "prueba8", "datasetDesc", "", DatasetTypes.File,
      Some(mockFileInput), None, None, None, 1, enabled = true, Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
      "yyyy/mm/dd", Some(true), IngestionTypes.LastChanges, database, tableName, ValidationTypes.FailFast, permissiveThreshold,
      PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None, None)

    val mockSource = Source(Some(1), "source8", "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

    val configManager: ConfigurationManager = mock[ConfigurationManager]
    val datalakeProps = DatalakeProperties("AccountName1", testResourcesBasePath, "container1")
    val landingProperties = LandingProperties("AccountName1", testResourcesBasePath, "container1", "bronze/landing/pending", "unknownDirectory1"
      , "invalidDirectory1", "corruptedDirectory1", "schemaMismatchDirectory", "streaming")

    // When
    when(configManager.getEnvironment).thenReturn(EnvironmentTypes.Local)
    when(configManager.getDatalakeProperties).thenReturn(datalakeProps)
    when(configManager.getLandingProperties).thenReturn(landingProperties)

    val datalakeManager = spy(new DatalakeManager(configManager,
      new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime)))
    val bronzeEngine = new BronzeEngine(datalakeManager, metaLogManager)

    when(metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(metaLogManager.finishDatasetLog(mockDatasetLog)).thenReturn(mockDatasetLog)

    // When
    bronzeEngine.process(mockSource)

    // Then
    verify(datalakeManager, times(1)).getPendingFiles(mockSource)
    verify(datalakeManager, times(1)).readPendingActualSchema(anyString, any[CsvFormat])
    verify(datalakeManager, times(1)).readPending(anyString, any[Dataset])
    verify(datalakeManager, times(1)).writeBronze(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, times(1)).moveFileToLandingArchive(anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).writeLandingError(any[DataFrame], anyString, anyString, any[CsvFormat], anyString, anyString)
    verify(datalakeManager, never).copyFileToError(anyString, anyString, anyString, anyString)

    verify(metaLogManager, times(1)).initIngestionLog(MetaInfo.Layers.BronzeLayerName)
    verify(metaLogManager, times(1)).initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, mockSource.sourceId.get)
    verify(metaLogManager, times(1)).initDatasetLog(mockSourceLog.sourceLogId, mockDataset.specificationId.get)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).finishDatasetLog(mockDatasetLog)
  }
}
