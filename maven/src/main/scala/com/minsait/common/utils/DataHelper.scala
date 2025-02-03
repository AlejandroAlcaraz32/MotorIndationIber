package com.minsait.common.utils

import com.minsait.common.configuration.models.DatalakeOutputTypes.Parquet
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models.{DatalakeOutputTypes, IndationProperties}
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.indation.metadata.models.JdbcConnection
//import com.minsait.common.utils.FileNameUtils.GoldDirectory
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.metagold.activity.exceptions.OutputTypeException
import com.minsait.metagold.logging.Logging
import com.minsait.metagold.metadata.models.SQLConnection
import com.minsait.metagold.metadata.models.enums.WriteModes._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.Try

object DataHelper
  extends SparkSessionWrapper with Logging {

  /**
   * Reads Data Lake Gold table into a Spark Dataframe
   *
   * @param metagoldProperties Generic configuration
   * @param tableRelativePath  Relative path to read from
   * @return Spark Dataframe from input data
   */
  def readGoldTable(
                     metagoldProperties: IndationProperties,
                     tableRelativePath: String
                   ): DataFrame = {
    val goldFilePath = getDlkPath(metagoldProperties, tableRelativePath, metagoldProperties.datalake.goldPath )//GoldDirectory)

    spark
      .read
      .format(getOutputType(metagoldProperties.datalake.outputType.getOrElse(DatalakeOutputTypes.Delta)))
      .load(goldFilePath)
  }

  /**
   * Reads Data Lake Gold table using Spark Metastore into a Spark Dataframe
   *
   * @param tableName Relative path to read from
   * @return Spark Dataframe from input data
   */
  def readSilverTable(metagoldProperties: IndationProperties, tablePath: String): DataFrame = {
    spark.table(tablePath) // avicom: no funciona en local porque lee del metastore de Hive
    //    val silverFilePath = getDlkPath(metagoldProperties,tableName, SilverDirectory)
    //
    //    spark
    //      .read
    //      .format(getOutputType(metagoldProperties.datalake.outputType.get))
    //      .load(silverFilePath)
  }

  /**
   * Checks if given saveMode is accepted and parses it to Spark SaveMode
   *
   * @param saveMode Write mode (append, truncate, overwrite)
   * @return Spark SaveMode object
   */
  def getSaveMode(saveMode: WriteMode): SaveMode = {
    saveMode match {
      case AppendMode =>
        SaveMode.Append
      case OverwriteMode =>
        SaveMode.Overwrite
      case TruncateMode =>
        SaveMode.Overwrite
      case ViewMode =>
        throw new UnsupportedOperationException(s"ViewMode only supported for temporary views, not a writting mode")
      case other =>
        throw new UnsupportedOperationException(s"WriteMode not supported $other.")
    }
  }

  /**
   * Checks if given outputType is accepted and returns its value
   *
   * @param outputType Output type object (delta, parquet)
   * @return String value of outputType if it is accepted
   */
  def getOutputType(outputType: DatalakeOutputTypes.DatalakeOutputType): String = {
    outputType match {
      case DatalakeOutputTypes.Delta => outputType.value
      case DatalakeOutputTypes.Parquet => outputType.value
      case other => throw new OutputTypeException(s"OuputType not supported ${other.value}.")
    }
  }

  /**
   * Writes output data files to the Data Lake
   *
   * @param metagoldProperties Generic configuration
   * @param dataFrame          Spark DataFrame containing output data
   * @param tableRelativePath  Relative path to write output files
   * @param saveMode           Write mode (append, truncate, overwrite)
   * @param partitionBy        String with output partitions separated by '/'
   */
  def writeDlkTable(metagoldProperties: IndationProperties,
                    dataFrame: DataFrame,
                    tableRelativePath: String,
                    tableName: String,
                    saveMode: WriteMode,
                    partitionBy: Option[String]) = {

    val partitions = if (partitionBy.isDefined && partitionBy.get.nonEmpty) partitionBy.get.split("/") else Array[String]()
    val goldFilePath = getDlkPath(metagoldProperties, tableRelativePath, metagoldProperties.datalake.goldPath) //GoldDirectory)

    // En caso de overwrite borramos físicamente la carpeta de destino
    if (saveMode == OverwriteMode) {

      // Modo overwrite, se borra primero
      val fsHelper: FSCommands = FSCommandsFactory.getFSCommands(metagoldProperties.environment)
      if (fsHelper.exists(goldFilePath)) {
        fsHelper.rm(goldFilePath)
      }

    }

    // En caso de modo vista, sólo se crea una vista temporal
    if (saveMode == ViewMode) {
      {
        dataFrame
          .createOrReplaceTempView(tableName)
        SparkSessionFactory.temporaryViews = SparkSessionFactory.temporaryViews ++ List(tableName)
      }
      // En cualquier otro caso, se guarda en el destino
    } else {
      dataFrame
        .write
        .format(getOutputType(metagoldProperties.datalake.outputType.getOrElse(DatalakeOutputTypes.Delta)))
        .partitionBy(partitions: _*)
        .mode(getSaveMode(saveMode))
        .option("truncate", saveMode == TruncateMode)
        .option("mergeschema", true) //TODO: Evaluar si la opción mergeschema es la opción adecuada
        .save(goldFilePath)
    }
  }

  /**
   * Returns Data Lake's absolute path from relative path
   *
   * @param metagoldProperties Generic configuration
   * @param tableRelativePath  Relative path
   * @param layerDirectory     Layer directory (bronze, silver, gold)
   * @return Absolute path
   */
  def getDlkPath(
                  metagoldProperties: IndationProperties,
                  tableRelativePath: String,
                  layerDirectory: String
                ): String = {
    s"${metagoldProperties.datalake.basePath.stripSuffix("/")}/${layerDirectory.stripSuffix("/")}/${tableRelativePath}"
  }

  /**
   * Writes output data to Data Base using JDBC connection
   *
   * @param sqlConnection      Data Base connection properties
   * @param tableName          Output table name
   * @param saveMode           Write mode (append, truncate, overwrite)
   * @param dataFrame          Spark DataFrame containing output data
   * @param metagoldProperties Generic configuration
   */
  def writeSQLTable(sqlConnection: SQLConnection,
                    tableName: String,
                    saveMode: WriteMode,
                    dataFrame: DataFrame,
                    metagoldProperties: IndationProperties
                   ) = {

    val url = sqlConnection.getJdbcUrl(metagoldProperties)
    val connectionProperties = getJdbcBaseConnectionProperties(metagoldProperties, sqlConnection)
    
    // Repartition the DataFrame to optimize the write performance
    val partitionedDataFrame = dataFrame.repartition(16)
    logger.info(s"Writing table $tableName connection: $url ...")
    // Write the DataFrame to the specified table with optimized options
    partitionedDataFrame.write
      .mode(getSaveMode(saveMode))
      .option("truncate", (saveMode == TruncateMode))
      .option("batchsize", 4000)
      .option("numPartitions", 16)
      .jdbc(url, tableName, connectionProperties)

    //if(connectionProperties.getProperty("Driver").equals("com.microsoft.sqlserver.jdbc.SQLServerDriver")){
      //if (connectionProperties.getProperty("AccessToken") != null) {
      //  logger.info("url: "+url)
      //  logger.info("tableName: "+tableName)
      //  dataFrame
      //    .write
      //    .format("com.microsoft.sqlserver.jdbc.spark")
      //    .mode(getSaveMode(saveMode))
      //    .option("truncate", (saveMode == TruncateMode))
      //    .option("url", url)
      //    .option("dbtable", tableName)
      //    .option("AccessToken", connectionProperties.getProperty("AccessToken"))
      //    .save()
      //}
      //else {
      //  dataFrame
      //    .write
      //    .format("com.microsoft.sqlserver.jdbc.spark")
      //    .mode(getSaveMode(saveMode))
      //    .option("truncate", (saveMode == TruncateMode))
      //    .option("url", url)
      //    .option("dbtable", tableName)
      //    .option("user", connectionProperties.getProperty("user"))
      //    .option("password", connectionProperties.getProperty("password"))
      //    .save()
      //}
    //}
    //else {
    //dataFrame
    //  .write
    //  .mode(getSaveMode(saveMode))
    //  .option("truncate", (saveMode == TruncateMode))
    //  .jdbc(url, tableName, connectionProperties)
    //}
  }

  /**
   * Reads table from a Data Base using JDBC into a Spark Dataframe
   *
   * @param sqlConnection      Data Base connection properties
   * @param sqlTable           Input table name
   * @param sqlExpr            Optional SQL query to get input data
   * @param metagoldProperties Generic configuration
   * @return Spark Dataframe from input data
   */
  def readSQLTable(sqlConnection: SQLConnection,
                   sqlTable: Option[String],
                   sqlExpr: Option[String],
                   metagoldProperties: IndationProperties
                  ): DataFrame = {
    if (sqlTable.isEmpty && sqlExpr.isEmpty)
      throw new UnsupportedOperationException(s"No table or sql expression defined for SQL table read")
    val url = sqlConnection.getJdbcUrl(metagoldProperties)
    val tableName = if (sqlExpr.isDefined) {
      s"(${sqlExpr.get}) as sqlTableExpr"
    } else {
      sqlTable.get
    }
    val connectionProperties = getJdbcBaseConnectionProperties(metagoldProperties, sqlConnection)
    spark.read.jdbc(url, tableName, connectionProperties)
  }

  /**
   * Extracts JDBC connection properties from generic configuration
   *
   * @param metagoldProperties
   * @return JDBC connection properties
   */
  def getJdbcBaseConnectionProperties(metagoldProperties: IndationProperties, sqlConnection: SQLConnection, forceSQLServer: Boolean=false): Properties = {

    val connectionProperties = new Properties

    //TODO: A futuro habrá que indicar tipos de destino
    val driverClass =
      if (metagoldProperties.environment == Local && !forceSQLServer) {
        // En modo local (tests) accedemos a h2
        "org.h2.Driver"
      }
      else {
        // En modo productivo, estamos en modo Azure SQL Database
        "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      }
    connectionProperties.setProperty("Driver", driverClass)

    // En caso de no tener usuario y password es señal de que vamos a modo service principal (preferido para entorno productivo)
    // en caso contrario, establecemos valores de usuario y contraseña
    if (sqlConnection.userKey.isDefined && sqlConnection.passwordKey.isDefined){
      // usuario y password
      connectionProperties.put("user", metagoldProperties.getSecret(sqlConnection.userKey.get))
      connectionProperties.put("password", metagoldProperties.getSecret(sqlConnection.passwordKey.get))
    }
    else {
      // access token
      connectionProperties.put("AccessToken", metagoldProperties.getAcessToken())
    }

    connectionProperties
  }

  /**
   * Truncates Data Base table
   *
   * @param sqlConnection      Data Base connection properties
   * @param metagoldProperties Generic configuration
   * @param table              Table to truncate
   */
  def truncateTable(sqlConnection: SQLConnection,
                    metagoldProperties: IndationProperties,
                    table: String): Unit = {
    executeNonQuery(sqlConnection, metagoldProperties, s"truncate table $table")
  }

  /**
   * Drops Data Base Table
   *
   * @param sqlConnection      Data Base connection properties
   * @param metagoldProperties Generic configuration
   * @param table              Table to drop
   */
  def dropTable(sqlConnection: SQLConnection,
                metagoldProperties: IndationProperties,
                table: String): Unit = {
    executeNonQuery(sqlConnection, metagoldProperties, s"drop table $table")
  }

  /**
   * Gets java sql Connection from metadata connection
   * @param sqlConnection       Data Base connection properties
   * @param metagoldProperties  Generic configuration
   * @return
   */
  def getConnection(sqlConnection: SQLConnection,
                    metagoldProperties: IndationProperties): Connection={

    val url = sqlConnection.getJdbcUrl(metagoldProperties)
    DriverManager.getConnection(url, getJdbcBaseConnectionProperties(metagoldProperties,sqlConnection))
  }

  /**
   * Executes SQL statement on Data Base
   *
   * @param sqlConnection      Data Base connection properties
   * @param metagoldProperties Generic configuration
   * @param query              SQL statement to execute
   */
  def executeNonQuery(sqlConnection: SQLConnection,
                      metagoldProperties: IndationProperties,
                      query: String): Unit = {
    val connection =getConnection(sqlConnection, metagoldProperties)
    val stmt = connection.createStatement()
    stmt.execute(query)

    connection.close()
  }


  /**
   * Performs a full table reconstruction to avoid cached parquet tables in metastore not being refreshed after new data load.
   *
   * @param metagoldProperties : Generic configuration
   * @param database           : database name
   * @param table              : table name
   * @param pathToWrite        : full path to write
   */
  def checkTableRecreation(metagoldProperties: IndationProperties, database: String, table: String, pathToWrite: String) = {

    val tableName = s"${database}.${table}"

    // In case of Parquet and table existing
    val tableExists = Try {
      spark.read.table(tableName)
      true
    }.getOrElse(false)
    if (getOutputType(metagoldProperties.datalake.outputType.getOrElse(DatalakeOutputTypes.Delta)) == Parquet.value && tableExists) {

      // Get data from hive table and from path
      val dfParquet = spark.read.option("spark.sql.parquet.int96RebaseModeInRead", "LEGACY").option("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY").parquet(pathToWrite)
      val dfHive = spark.sql(s"select * from $tableName")

      // Check if schemas are different
      val newCols1 = dfParquet.columns.diff(dfHive.columns)
      val newCols2 = dfHive.columns.diff(dfParquet.columns)

      // If a column difference is detected, we must drop hive table and create it again
      if (newCols1.length > 0 || newCols2.length > 0) {

        logger.info(s"Reconstructing table $tableName in Hive catalog for schema evolution")

        if (spark.catalog.isCached(tableName)) {
          spark.catalog.uncacheTable(tableName)
        }

        spark.sql(s"DROP TABLE IF EXISTS $tableName")

        spark.catalog.clearCache() //TODO: Revisar - esta operación es peligrosa, podría afectar a otras operaciones en ejecución

        spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING PARQUET LOCATION '" + pathToWrite + "'")

        repairPartitions(metagoldProperties, tableName)

        spark.catalog.refreshTable(tableName)
      }
    }
  }

  /**
   * This method repair table partition information in parquet tables
   *
   * @param metagoldProperties : Generic configurtion
   * @param tableName          : Table name in format database.table
   * @return
   */
  def repairPartitions(metagoldProperties: IndationProperties, tableName: String) = {
    try {
      // In case of Parquet only
      if (metagoldProperties.datalake.outputType.get == Parquet) {
        spark.sql("MSCK REPAIR TABLE " + tableName)
      }
    } catch {
      case ex: Throwable => logger.warn(ex.getMessage)
    }
  }

  def tableExists(tableName: String): Boolean = {
    val tableParts = tableName.split('.')
    val tableExistsHelp = Try {
      spark.read.table(tableName)
      true
    }.getOrElse(false)
    if (tableParts.length == 2) {
      val databaseName = tableParts(0)
      spark.catalog.databaseExists(databaseName) && tableExistsHelp
    } else {
      tableExistsHelp
    }
  }
}
