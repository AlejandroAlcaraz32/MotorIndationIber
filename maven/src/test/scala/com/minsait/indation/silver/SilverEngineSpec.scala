package com.minsait.indation.silver

import com.minsait.common.configuration.models._
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.datalake.exceptions.CreateDatabaseException
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums._
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.metalog.models.enums.ReprocessTypes.ReprocessType
import com.minsait.indation.metalog.models.{DatasetLog, Ingestion, SourceLog}
import com.minsait.indation.metalog.{MetaInfo, MetaLogManager}
import com.minsait.indation.silver.helper.PartitionHelper
import org.apache.spark.sql.DataFrame
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp


class SilverEngineSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  import spark.implicits._

  private val sourceName = "testsilver"
  private val database = "database"
  private val testResourcesBasePath = this.getClass.getResource("/").getPath

  private val mockSpecInput = FileInput(FileFormatTypes.Csv, "file_<yyyy><mm><dd>.csv", Some(CsvFormat("UTF-8", "|", CsvHeaderTypes.FirstLine, None,
    Some(false), None, None)), None, None, None)

  private val mockSpecSchema = SchemaColumns(
    List(
      Column("integer", ColumnsTypes.Integer, None,None, None,None, None, None, None, sensitive = false),
      Column("float", ColumnsTypes.Float, None,None, None,None, None, Some(ColumnTransformation(ColumnTransformationTypes.Comma, None, None)), None,
        sensitive = false),
      Column("string", ColumnsTypes.String, None,None, None,None, None, None, None, sensitive = false),
      Column("long", ColumnsTypes.Long, None,None, None,None, None, None, None, sensitive = false),
      Column("boolean", ColumnsTypes.Boolean, None,None, None,None, None, None, None, sensitive = false),
      Column("double", ColumnsTypes.Double, None,None, None,None, None, None, None, sensitive = false),
      Column("decimal", ColumnsTypes.Decimal, None,None, None,None, None, None, Some(ColumnDecimalParameters(5, 2)), sensitive = false),
      Column("date", ColumnsTypes.DateTime, None,None, None, None,None, Some(ColumnTransformation(ColumnTransformationTypes.Date, Some("dd-MM-yyyy"), None)), None,
        sensitive = false)
    )
  )

  private val mockSpecSchema2 = SchemaColumns(
    List(
      Column("integer", ColumnsTypes.DateTime, None,None, None,None, None, None, None, sensitive = false),
      Column("float", ColumnsTypes.Float, None,None, None,None, None, None, None, sensitive = false),
      Column("string", ColumnsTypes.Integer, None,None, None,None, None, None, None, sensitive = false),
      Column("long", ColumnsTypes.Long, None,None, None, None,None, None, None, sensitive = false),
      Column("boolean", ColumnsTypes.String, None,None, None,None, None, None, None, sensitive = false),
      Column("double", ColumnsTypes.Double, None,None, None,None, None, Some(ColumnTransformation(ColumnTransformationTypes.Comma, None, None)), None,
        sensitive = false),
      Column("decimal", ColumnsTypes.Decimal, None,None, None,None, None, None, Some(ColumnDecimalParameters(5, 2)), sensitive = false),
      Column("date", ColumnsTypes.DateTime, None,None, None, None,None, Some(ColumnTransformation(ColumnTransformationTypes.Date, Some("dd-MM-yyyy"), None)), None,
        sensitive = false)
    )
  )

  private val mockDataset = Dataset(Some(1), Some(1), Some(1), "test", "datasetDesc", sourceName, DatasetTypes.File,
    Some(mockSpecInput), None, None, None, 1, enabled = true,  Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
    "yyyy/mm/dd", Some(true), IngestionTypes.FullSnapshot, "database1", "tableName1", ValidationTypes.Permissive, 2,
    PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema), None,None)
  private val mockSource = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset))

  private val mockDataset2 = Dataset(Some(1), Some(1), Some(1), "test", "datasetDesc", sourceName, DatasetTypes.File,
    Some(mockSpecInput), None, None, None, 1, enabled = true,  Some(Timestamp.valueOf("2020-01-01 00:00:00.0")), createDatabase = true, ClassificationTypes.Public,
    "yyyy/mm/dd", Some(true), IngestionTypes.FullSnapshot, "database1", "tableName1", ValidationTypes.Permissive, 2,
    PermissiveThresholdTypes.Absolute, SchemaDefinitionTypes.Columns, Some(mockSpecSchema2), None,None)

  private val mockSource2 = Source(Some(1), sourceName, "sourceDesc", SourceTypes.Directory, None, None, List(mockDataset2))

  private val mockIngestion = Ingestion(1, Some(MetaInfo.Layers.SilverLayerName))
  private val mockSourceLog = SourceLog(1, 1, 1)
  private val mockDatasetLog = DatasetLog(1,
    1,
    1,
    0,
    Some(0),
    Some(0),
    MetaInfo.Statuses.FinishState,
    MetaInfo.Result.OK,
    Some("Test DatasetLog"),
    Some("partition"),
    Some(ReprocessTypes.None),
    Some(10),
    Some(2),
    Some(testResourcesBasePath + "samples/testsilver"),
    Some(""),
    Some(""),
    Some(true),
    None
  )

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val indationProperties = IndationProperties(
    MetadataProperties("","","","")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("","","","","","","","","")
    , DatalakeProperties("","","")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties())
    , EnvironmentTypes.Local,
    tmpDirectory
  )

  override def beforeAll() {
    SparkSessionFactory.configSparkSession(indationProperties)
  }

  test("SilverEngine#process not pending ingestion") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    mockIngestion.result = MetaInfo.Result.OK

    //WHEN
    when(metaLogManager.getPendingSilver(mockSource.sourceId.get)).thenReturn(scala.List.empty[DatasetLog])
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)

    silverEngine.process(mockSource)

    //THEN
    verify(metaLogManager, never).initDatasetLog(anyInt, anyInt)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)
  }

  test("SilverEngine#process finish when exists running execution") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    mockIngestion.result = MetaInfo.Result.OK

    //WHEN
    when(metaLogManager.getPendingSilver(mockSource.sourceId.get)).thenReturn(scala.List.empty[DatasetLog])
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(true)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)

    silverEngine.process(mockSource)

    //THEN
    verify(metaLogManager, never).getPendingSilver(anyInt)
    verify(metaLogManager, never).initDatasetLog(anyInt, anyInt)
    verify(metaLogManager, never).finishSourceLog(mockSourceLog)
    verify(metaLogManager, never).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)

  }

  test("SilverEngine#process pending ingestion file with more wrong columns than threshold in absolute mode") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilver"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK


    val df2 = spark.read.format("delta").load(dflocation)

    //WHEN
    when(metaLogManager.getPendingSilver(mockSource.sourceId.get)).thenReturn(pedingSilver)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(datalakeManager.readBronze(dflocation)).thenReturn(df2)

    val ingestionLog2 = silverEngine.process(mockSource)

    //THEN
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).updatePendingSilverToFalse(pedingSilver.head)
    assert(ingestionLog2.result == MetaInfo.Result.WithErrors)

  }

  test("SilverEngine#process pending ingestion file with more wrong columns than threshold in percentage mode") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilver"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK


    val df2 = spark.read.format("delta").load(dflocation)

    //WHEN
    when(metaLogManager.getPendingSilver(mockSource2.sourceId.get)).thenReturn(pedingSilver)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource2.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource2.sourceId.get)).thenReturn(false)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(datalakeManager.readBronze(dflocation)).thenReturn(df2)

    val ingestionLog2 = silverEngine.process(mockSource2)

    //THEN
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).updatePendingSilverToFalse(pedingSilver.head)
    assert(ingestionLog2.result == MetaInfo.Result.WithErrors)

  }

  test("SilverEngine#process pending ingestion all ok") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilverok_20200524"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK


    val df3 = spark.read.format("delta").load(dflocation)


    //WHEN
    when(metaLogManager.getPendingSilver(mockSource.sourceId.get)).thenReturn(pedingSilver)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(datalakeManager.readBronze(dflocation)).thenReturn(df3)

    val ingestionLog3 = silverEngine.process(mockSource)

    //THEN
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(datalakeManager, times(1)).writeSilver(any[IngestionTypes.IngestionType], anyString
      , anyString, any[DataFrame], anyString, any[List[String]], anyBoolean, anyString, anyString, any[ReprocessType],anyBoolean, anyString)
    verify(datalakeManager, never).writeQAError(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).updatePendingSilverToFalse(pedingSilver.head)
    assert(ingestionLog3.result == MetaInfo.Result.OK)

  }

  test("SilverEngine#process pending ingestion limit error not exceeded") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilvernex"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK


    val df4 = spark.read.format("delta").load(dflocation)


    //WHEN
    when(metaLogManager.getPendingSilver(mockSource.sourceId.get)).thenReturn(pedingSilver)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(datalakeManager.readBronze(dflocation)).thenReturn(df4)

    val ingestionLog4 = silverEngine.process(mockSource)

    //THEN
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(datalakeManager, times(1)).writeSilver(any[IngestionTypes.IngestionType], anyString, anyString, any[DataFrame]
      , anyString, any[List[String]], anyBoolean, anyString, anyString, any[ReprocessType],anyBoolean, anyString)
    verify(datalakeManager, times(1)).writeQAError(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).updatePendingSilverToFalse(pedingSilver.head)
    assert(ingestionLog4.result == MetaInfo.Result.WithErrors)

  }

  test("SilverEngine#process pending ingestion schema not match") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilvernex"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK


    val df4 = spark.read.format("delta").load(dflocation)


    //WHEN
    when(metaLogManager.getPendingSilver(mockSource2.sourceId.get)).thenReturn(pedingSilver)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource2.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(datalakeManager.readBronze(dflocation)).thenReturn(df4)

    val ingestionLog4 = silverEngine.process(mockSource2)

    //THEN
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(datalakeManager, never).writeSilver(any[IngestionTypes.IngestionType], anyString, anyString, any[DataFrame], anyString
      , any[List[String]], anyBoolean, anyString, anyString, any[ReprocessType],anyBoolean, anyString)
    verify(datalakeManager, times(1)).writeQAError(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).updatePendingSilverToFalse(pedingSilver.head)
    assert(ingestionLog4.result == MetaInfo.Result.WithErrors)

  }

  test("SilverEngine#process pending ingestion not same columns") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilvernex"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK


    val df5 = spark.read.format("delta").load(dflocation)


    //WHEN
    when(metaLogManager.getPendingSilver(mockSource2.sourceId.get)).thenReturn(pedingSilver)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource2.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)
    when(datalakeManager.readBronze(dflocation)).thenReturn(df5)

    val ingestionLog5 = silverEngine.process(mockSource2)

    //THEN
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(datalakeManager, never).writeSilver(any[IngestionTypes.IngestionType], anyString, anyString, any[DataFrame], anyString
      , any[List[String]], anyBoolean, anyString, anyString, any[ReprocessType],anyBoolean, anyString)
    verify(datalakeManager, times(1)).writeQAError(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    verify(metaLogManager, times(1)).updatePendingSilverToFalse(pedingSilver.head)
    assert(ingestionLog5.result == MetaInfo.Result.WithErrors)

  }

  test("SilverEngine#process wrong database configuration should throw a CreateDatabaseException") {

    // Given
    val datalakeManager: DatalakeManager = mock[DatalakeManager]
    val metaLogManager: MetaLogManager = mock[MetaLogManager]
    val silverEngine = new SilverEngine(datalakeManager, metaLogManager)
    val pedingSilver: scala.List[DatasetLog] = List(mockDatasetLog)
    val dflocation = testResourcesBasePath + "samples/testsilverok_20200524"
    mockDatasetLog.outputPath = Some(dflocation)
    mockIngestion.result = MetaInfo.Result.OK
    val inputDF = spark.read.format("delta").load(dflocation)
    val mockDataBaseException = new CreateDatabaseException("Create database configuration is set to false, " +
      "but datalake path and/or database do not exists for " + database + " database.")

    when(metaLogManager.getPendingSilver(mockSource.sourceId.get)).thenReturn(pedingSilver)
    when(datalakeManager.readBronze(dflocation)).thenReturn(inputDF)
    when(datalakeManager.writeSilver(any[IngestionTypes.IngestionType], anyString, anyString,
      any[DataFrame], anyString, any[List[String]], anyBoolean, anyString, anyString, any[ReprocessType],anyBoolean, anyString)).thenThrow(mockDataBaseException)
    when(metaLogManager.existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)).thenReturn(false)
    when(metaLogManager.initIngestionLog(MetaInfo.Layers.SilverLayerName)).thenReturn(mockIngestion)
    when(metaLogManager.initSourceLog(mockIngestion.ingestionId, mockSource.sourceId.get)).thenReturn(mockSourceLog)
    when(metaLogManager.initDatasetLog(anyInt, anyInt)).thenReturn(mockDatasetLog)
    when(metaLogManager.finishSourceLog(mockSourceLog)).thenReturn(mockSourceLog)
    when(metaLogManager.finishIngestionLog(mockIngestion)).thenReturn(mockIngestion)

    // When
    val ingestionLog = silverEngine.process(mockSource)

    // Then
    verify(metaLogManager, times(1)).existsConcurrentExecution(MetaInfo.Layers.SilverLayerName, mockSource.sourceId.get)
    verify(datalakeManager, times(1)).readBronze(dflocation)
    verify(datalakeManager, times(1)).writeSilver(any[IngestionTypes.IngestionType], anyString, anyString, any[DataFrame]
      , anyString, any[List[String]], anyBoolean, anyString, anyString, any[ReprocessType],anyBoolean, anyString)
    verify(datalakeManager, never).writeQAError(any[DataFrame], anyString, anyString, anyString, anyString)
    verify(datalakeManager, never).deleteBronzeData(anyString)
    verify(metaLogManager, times(1)).finishSourceLog(mockSourceLog)
    verify(metaLogManager, times(1)).finishIngestionLog(mockIngestion)
    assert(ingestionLog.result == MetaInfo.Result.WithErrors)
  }

  test("SilverEngine#process test getReplaceWhereFromPartitions should return an expected query"){
    val partitions = "p1/p2/p3/p4"
    val partitionsArray = partitions.split("/")

    val expectedReplaceWhere = "(p1 = 'partitionCol1' AND p2 = 'partitionCol2' AND p3 = 'partitionCol3' AND p4 = 'partitionCol4')"

    val initialDF = Seq(
      ("A", "partitionCol1", "partitionCol2", "partitionCol3", "partitionCol4")
    ).toDF("col1", "p1", "p2", "p3", "p4")

    val replaceWhere = PartitionHelper.getReplaceWhereFromPartitions(initialDF, partitionsArray)

    assert(expectedReplaceWhere == replaceWhere)
  }
}
