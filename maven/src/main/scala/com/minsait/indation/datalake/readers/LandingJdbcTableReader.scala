package com.minsait.indation.datalake.readers

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.enums.ColumnsTypes
import com.minsait.indation.metadata.models.enums.IngestionTypes.{Incremental, LastChanges}
import com.minsait.indation.metadata.models.enums.JdbcAuthenticationTypes.{ServicePrincipal, UserPassword}
import com.minsait.indation.metadata.models.enums.JdbcDriverTypes.{MSSql, Oracle}
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import java.util.Properties

trait LandingJdbcTableReader {
  def readTable(source: Source, dataset: Dataset, indationProperties: IndationProperties, maxTimestamp: Option[String]): DataFrame
}

object LandingJdbcTableReader extends LandingJdbcTableReader with SparkSessionWrapper  {

  //  def apply(source: Source, dataset: Dataset): LandingJdbcTableReader = source.jdbcConnection.get.driver  match {
  //    case Oracle => LandingJdbcOracleTableReader
  //    case MSSql => LandingJdbcMssqlTableReader
  //    case _ => throw new UnsupportedOperationException("Unsupported Jdbc Table Input Driver " + source.jdbcConnection.get.driver)
  //  }

  override def readTable(source: Source, dataset: Dataset, indationProperties: IndationProperties, maxTimestamp: Option[String]): DataFrame = {

    val tableConfig = dataset.tableInput.get
    val connectionProperties=getConnectionPartitionProperties(source,dataset,indationProperties)

    // Configuración de lectura
    val url = getConnectionUrl(source, indationProperties)

    val dbTable = if (tableConfig.onlyNewData.getOrElse(false) && tableConfig.query.isDefined) {
      if (maxTimestamp.isDefined) { //La tabla ya tiene algún dato, por tanto se pude coger lo nuevo respecto a eso
        val query = tableConfig.getTable

        query.replace("isTimestampMaxSilverValue", maxTimestamp.get)
      } else { //Primera carga de la tabla, por lo que deberá leer la tabla completa de origen
        tableConfig.getTableNoQuery
      }
    } else {
      tableConfig.getTable
    }

    // Lectura
    //    Try(
    spark
      .read
      .jdbc(url,
        dbTable,
        connectionProperties
      )
    //    )
  }

  def getConnectionUrl(source: Source, indationProperties: IndationProperties):String={
    val connectionConfig = source.jdbcConnection.get

    // Configuración de lectura
    connectionConfig.getConnectionUrl(indationProperties)
  }

  def getConnectionProperties(source: Source,
                              dataset: Dataset,
                              indationProperties: IndationProperties): Properties={

    val connectionProperties = new Properties

    // Driver
    connectionProperties.setProperty("Driver", source.jdbcConnection.get.driver.value)

    // Authentication
    source.jdbcConnection.get.authenticationType match {
      case UserPassword =>{
        connectionProperties.put("user", source.jdbcConnection.get.getUser(indationProperties))
        connectionProperties.put("password", source.jdbcConnection.get.getPassword(indationProperties))
      }
      case ServicePrincipal =>{
        val accessToken = indationProperties.getAcessToken()
        connectionProperties.put("AccessToken", accessToken)
      }
      case other =>
        throw new UnsupportedOperationException(s"JDBC Authentication Type not supported ${source.jdbcConnection.get.authenticationType}")
    }
    // Devolución
    connectionProperties
  }

  def getConnectionPartitionProperties(source: Source,
                                       dataset: Dataset,
                                       indationProperties: IndationProperties): Properties={

    // Iniciamos con los parámetros base
    val connectionProperties = getConnectionProperties(source,dataset,indationProperties)

    // Comprobación de parámetro de fetchsize, que determina cuantas filas traer por viaje de ida y vuelta a la bbdd
    if (dataset.tableInput.get.fetchsize.isDefined) {
      connectionProperties.put("fetchsize", dataset.tableInput.get.fetchsize.get)
    }

    // Comprobación de parámetro de paralelismo
    if (dataset.tableInput.get.numPartitions.isDefined){
      connectionProperties.put("numPartitions", dataset.tableInput.get.numPartitions.get)

      // Parámetros extra de particionado
      if (dataset.tableInput.get.partitionColumn.isDefined){
        connectionProperties.put("partitionColumn",dataset.tableInput.get.partitionColumn.get)

        val column = dataset.getColumnByName(dataset.tableInput.get.partitionColumn.get)

        // Al establecer columna de partición, se requieren los límites
        val lowerBound =
          executeScalar(
            source,
            s"(select min(${column.name}) c from ${dataset.tableInput.get.getTable}) t",
            getConnectionProperties(source,dataset,indationProperties),
            indationProperties
          )

        val upperBound =
          executeScalar(
            source,
            s"(select max(${dataset.tableInput.get.partitionColumn.get}) c from ${dataset.tableInput.get.getTable}) t",
            getConnectionProperties(source,dataset,indationProperties),
            indationProperties
          )

        column.typ match {
          case ColumnsTypes.Integer =>
            connectionProperties.put("lowerBound",lowerBound.toString)
            connectionProperties.put("upperBound", upperBound.toString)
          case ColumnsTypes.Short =>
            connectionProperties.put("lowerBound",lowerBound.toString)
            connectionProperties.put("upperBound", upperBound.toString)
          case ColumnsTypes.Float=>
            connectionProperties.put("lowerBound",lowerBound.toString)
            connectionProperties.put("upperBound",upperBound.toString)
          case ColumnsTypes.Long=>
            connectionProperties.put("lowerBound",lowerBound.toString)
            connectionProperties.put("upperBound",upperBound.toString)
          case ColumnsTypes.Double=>
            connectionProperties.put("lowerBound",lowerBound.toString)
            connectionProperties.put("upperBound",upperBound.toString)
          case ColumnsTypes.Decimal=>
            connectionProperties.put("lowerBound",lowerBound.toString)
            connectionProperties.put("upperBound",upperBound.toString)
          //          case ColumnsTypes.Date=>
          //            val lb = new org.joda.time.DateTime(lowerBound.asInstanceOf[java.sql.Timestamp].getTime)
          //            val ub = new org.joda.time.DateTime(upperBound.asInstanceOf[java.sql.Timestamp].getTime)
          //            connectionProperties.put("lowerBound", lb.minusDays(1).toString("yyyyMMdd"))
          //            connectionProperties.put("upperBound", ub.plusDays(1).toString("yyyyMMdd"))
          //            connectionProperties.put("lowerBound", lowerBound.asInstanceOf[java.sql.Timestamp])
          //            connectionProperties.put("upperBound", upperBound.asInstanceOf[java.sql.Timestamp])
          //            connectionProperties.put("lowerBound",lowerBound.toString)
          //            connectionProperties.put("upperBound",upperBound.toString)
          case other =>
            throw new UnsupportedOperationException(s"Partition column type not supported $other")
        }
      }
    }

    // Devolución
    connectionProperties
  }

  def executeScalar(
                     source: Source,
                     sqlTableExpr: String,
                     connectionProperties: Properties,
                     indationProperties: IndationProperties): Any ={
    val df = spark
      .read
      .jdbc(getConnectionUrl(source, indationProperties),
        sqlTableExpr,
        connectionProperties
      )
    //    df.schema(0).dataType match {
    //      case DateType =>
    //
    //    }
    df.collect()(0).get(0)
  }

}