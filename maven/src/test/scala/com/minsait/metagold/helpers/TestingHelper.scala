package com.minsait.metagold.helpers

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.configuration.models.DatalakeOutputTypes.Delta
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.DataHelper
import com.minsait.common.utils.DataHelper.tableExists
//import com.minsait.common.utils.FileNameUtils.SilverDirectory
import com.minsait.common.utils.fs.LocalFSCommands
import com.minsait.metagold.metadata.MetadataFilesManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.io.File
import java.nio.file.Files

object TestingHelper extends SparkSessionWrapper {

  // Ruta de recursos
  val testResourcesBasePath = new File(this.getClass.getResource("/").getPath).toString.replace("\\","/") + "/"

  // Constants
  val pathToTestConfigParquet = testResourcesBasePath + "metagold/metagold-config-local-parquet.yml"
  val pathToTestConfigQ1Parquet = testResourcesBasePath + "metagold/metagold-config-local-q1-parquet.yml"
  val pathToTestConfigQ2Parquet = testResourcesBasePath + "metagold/metagold-config-local-q2-parquet.yml"
  val pathToTestConfigQ3Parquet = testResourcesBasePath + "metagold/metagold-config-local-q3-parquet.yml"

  val pathToTestConfigDelta = testResourcesBasePath + "metagold/metagold-config-local-delta.yml"
/*
  val pathToTestConfigParquetError= testResourcesBasePath + "metagold/metagold-config-local-parquet-error.yml"
*/
  val pathToPrivate = testResourcesBasePath + "metagold/datalake/gold/private"
  val pathToStatisticsParent = testResourcesBasePath + "metagold/datalake/gold/private/aplicaciones"
  val pathToStatistics = testResourcesBasePath + "metagold/datalake/gold/private/aplicaciones/transform"
  val pathMetadataerror = testResourcesBasePath + "metagold/metadataError"

  // Configuration Delta
  ReplaceInFile(pathToTestConfigDelta,"basePath: \"src/test/resources/metagold/datalake\"","basePath: \""+testResourcesBasePath+"metagold/datalake\"")
  val configManagerDelta = new ConfigurationManager(pathToTestConfigDelta)
  val indationPropertiesDelta=configManagerDelta.indationProperties//.copy(datalake = configManagerDelta.indationProperties.datalake.copy(basePath = s"${testResourcesBasePath}${configManagerDelta.indationProperties.datalake.basePath.replace(testsCommonPath,"")}"))
  val metadataFilesManagerDelta = new MetadataFilesManager(indationPropertiesDelta)

  // Configuration Parquet
  ReplaceInFile(pathToTestConfigParquet,"basePath: \"src/test/resources/metagold/datalake\"","basePath: \""+testResourcesBasePath+"metagold/datalake\"")
  val configManagerParquet = new ConfigurationManager(pathToTestConfigParquet)
  val indationPropertiesParquet=configManagerParquet.indationProperties//.copy(datalake = configManagerDelta.indationProperties.datalake.copy(basePath = s"${testResourcesBasePath}${configManagerDelta.indationProperties.datalake.basePath.replace(testsCommonPath,"")}"))
  val metadataFilesManagerParquet = new MetadataFilesManager(indationPropertiesParquet)

  // Configuration Parquet pathToTestConfigQ1Parquet
  ReplaceInFile(pathToTestConfigQ1Parquet,"basePath: \"src/test/resources/metagold/datalake\"","basePath: \""+testResourcesBasePath+"metagold/datalake\"")
  val configManagerQ1Parquet = new ConfigurationManager(pathToTestConfigQ1Parquet)
  val indationPropertiesQ1Parquet=configManagerParquet.indationProperties//.copy(datalake = configManagerDelta.indationProperties.datalake.copy(basePath = s"${testResourcesBasePath}${configManagerDelta.indationProperties.datalake.basePath.replace(testsCommonPath,"")}"))
  val metadataFilesManagerQ1Parquet = new MetadataFilesManager(indationPropertiesParquet)

  // Configuration Parquet pathToTestConfigQ2Parquet
  ReplaceInFile(pathToTestConfigQ2Parquet,"basePath: \"src/test/resources/metagold/datalake\"","basePath: \""+testResourcesBasePath+"metagold/datalake\"")
  val configManagerQ2Parquet = new ConfigurationManager(pathToTestConfigQ2Parquet)
  val indationPropertiesQ2Parquet=configManagerParquet.indationProperties//.copy(datalake = configManagerDelta.indationProperties.datalake.copy(basePath = s"${testResourcesBasePath}${configManagerDelta.indationProperties.datalake.basePath.replace(testsCommonPath,"")}"))
  val metadataFilesManagerQ2Parquet = new MetadataFilesManager(indationPropertiesParquet)

  // Configuration Parquet pathToTestConfigQ3Parquet
  ReplaceInFile(pathToTestConfigQ3Parquet,"basePath: \"src/test/resources/metagold/datalake\"","basePath: \""+testResourcesBasePath+"metagold/datalake\"")
  val configManagerQ3Parquet = new ConfigurationManager(pathToTestConfigQ3Parquet)
  val indationPropertiesQ3Parquet=configManagerParquet.indationProperties//.copy(datalake = configManagerDelta.indationProperties.datalake.copy(basePath = s"${testResourcesBasePath}${configManagerDelta.indationProperties.datalake.basePath.replace(testsCommonPath,"")}"))
  val metadataFilesManagerQ3Parquet = new MetadataFilesManager(indationPropertiesParquet)

  // SQL
  val schemaStaging = "staging"
  val schemaDatamart = "datamart"
  val tableProductAppend = "DimProductAppend"
  val tableProductTruncate = "DimProductTruncate"
  val tableProductOverwrite = "DimProductOverwrite"
  val tableAggProduct = "aggProduct"
  val sqlConnectionName = "testConnection"
  lazy val SQLConnection = metadataFilesManagerParquet.sqlConnectionByName(sqlConnectionName).get

  // Silver Data samples
  val dlkProductFolder = "public/database/DimProduct"
  val dlkProductCategoryFolder = "public/database/DimProductCategory"
  val dlkProductSubcategoryFolder = "public/database/DimProductSubcategory"
  val dlkFactInternetSalesFolder = "public/database/FactInternetSales"
  val silverProductTable = "database.DimProduct"
  val silverProductCategoryTable = "database.DimProductCategory"
  val silverProductSubcategoryTable = "database.DimProductSubcategory"
  val silverFactInternetSalesTable = "database.FactInternetSales"

  def ReplaceInFile(filePath: String, textToBeReplaced: String, textToReplace: String)={
    import java.nio.charset.StandardCharsets
    import java.nio.file.Paths

    val path = Paths.get(filePath)
    val charset = StandardCharsets.UTF_8

    var content = new String(Files.readAllBytes(path), charset)
    content = content.replaceAll(textToBeReplaced, textToReplace)
    Files.write(path, content.getBytes(charset))
  }

  // Inicialización de Spark
  def InitSparkSessionParquet={
    // SparkSession
    SparkSessionFactory.configSparkSession(indationPropertiesParquet)
  }
  def InitSparkSessionDelta={
    // SparkSession
    SparkSessionFactory.configSparkSession(indationPropertiesDelta)
  }

  // Inicialización de base de datos sql y metastore para pruebas
  def PrepareTestData(indationProperties: IndationProperties)={

    // Clean existing tables from previous tests
    DataHelper.executeNonQuery(SQLConnection, indationProperties,"drop all objects")


    // Schema creation in sql server
    DataHelper.executeNonQuery(
      SQLConnection,
      indationProperties,
      s"CREATE SCHEMA IF NOT EXISTS $schemaDatamart "
    )
    DataHelper.executeNonQuery(
      SQLConnection,
      indationProperties,
      s"CREATE SCHEMA IF NOT EXISTS $schemaStaging "
    )

    // Existing tables creation
    DataHelper.executeNonQuery(
      SQLConnection,
      indationProperties,
      s"CREATE TABLE IF NOT EXISTS $schemaStaging.[${TestingHelper.tableProductAppend}] (ProductKey int not null primary key, ProductName nvarchar(50) not null) "
    )
    DataHelper.executeNonQuery(
      SQLConnection,
      indationProperties,
      s"CREATE TABLE IF NOT EXISTS $schemaStaging.[${TestingHelper.tableProductOverwrite}] (ProductKey int not null primary key, ProductName nvarchar(50) not null) "
    )
    DataHelper.executeNonQuery(
      SQLConnection,
      indationProperties,
      s"CREATE TABLE IF NOT EXISTS $schemaStaging.[${TestingHelper.tableProductTruncate}] (ProductKey int not null primary key, ProductName nvarchar(50) not null)"
    )

    // Metastore initialization
    try {
      cleanFullMetastore()
    }catch {case ex: Throwable=>println("Empty metastore for testing")}

    spark.catalog.clearCache()

    spark.sql("CREATE DATABASE IF NOT EXISTS database")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $silverProductTable USING ${indationProperties.datalake.outputType.getOrElse(Delta).value.toUpperCase} LOCATION '${indationProperties.datalake.basePath}/${indationProperties.datalake.silverPath}/$dlkProductFolder'")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $silverProductCategoryTable USING ${indationProperties.datalake.outputType.getOrElse(Delta).value.toUpperCase} LOCATION '${indationProperties.datalake.basePath}/${indationProperties.datalake.silverPath}/$dlkProductCategoryFolder'")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $silverProductSubcategoryTable USING ${indationProperties.datalake.outputType.getOrElse(Delta).value.toUpperCase} LOCATION '${indationProperties.datalake.basePath}/${indationProperties.datalake.silverPath}/$dlkProductSubcategoryFolder'")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $silverFactInternetSalesTable USING ${indationProperties.datalake.outputType.getOrElse(Delta).value.toUpperCase} LOCATION '${indationProperties.datalake.basePath}/${indationProperties.datalake.silverPath}/$dlkFactInternetSalesFolder'")

  }

  def getDfSilverProduct(indationProperties: IndationProperties):DataFrame={
    DataHelper.readSilverTable(indationProperties, TestingHelper.silverProductTable)
      .select(col("ProductKey"),col("SpanishProductName").alias("ProductName"))
  }

  def assertDataframeEquals(expectedDf: DataFrame, actualDf: DataFrame)={
    assert(expectedDf.count()==actualDf.count())
    assert(expectedDf.columns.length == actualDf.columns.length)
    expectedDf.schema.map(expectedField => {
      val actualFields = actualDf.schema.filter(field => field.name==expectedField.name)
      assert(actualFields.length==1)
      val actualField = actualFields(0)
      assert(actualField.dataType==expectedField.dataType)
      assert(actualField.nullable==expectedField.nullable)
    })
    val columns = expectedDf.columns
    val dfExpectedNotFound = expectedDf.join(actualDf,columns.toSeq,"leftanti")
    val dfActualNotFound = actualDf.join(expectedDf,columns.toSeq,"leftanti")
    assert(dfExpectedNotFound.count()==0)
    assert(dfActualNotFound.count()==0)
  }

  def cleanStats()={
    if (tableExists("aplicaciones.transform")) {
      spark.sql("delete from aplicaciones.transform")
    }
    spark.sql("DROP DATABASE IF EXISTS aplicaciones CASCADE")
    if (LocalFSCommands.exists(pathToStatisticsParent)){
      LocalFSCommands.rm(pathToStatisticsParent)
    }
  }

  def cleanFullMetastore()={

    spark.catalog.listDatabases.collect().foreach(db => {
      if (db.name != "default") {
        spark.sql("DROP DATABASE IF EXISTS " + db.name + " CASCADE")
      }
    })

    if (LocalFSCommands.exists(pathToPrivate)){
      LocalFSCommands.rm(pathToPrivate)
    }
  }
}
