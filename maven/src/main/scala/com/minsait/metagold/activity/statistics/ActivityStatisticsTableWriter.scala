package com.minsait.metagold.activity.statistics

import com.minsait.common.configuration.models.DatalakeOutputTypes.{Delta, Parquet}
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.BuildInfo
import com.minsait.common.utils.SilverConstants._
import com.minsait.metagold.activity.statistics.models.{ActivityEngine, ActivityStatistics}
import com.minsait.metagold.logging.Logging
import com.minsait.common.utils.DataHelper.{checkTableRecreation, getOutputType, repairPartitions, tableExists}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, date_trunc, lit, to_timestamp}

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.util.Try

class ActivityStatisticsTableWriter(override val indationProperties: IndationProperties)
  extends ActivityStatisticsObserver with Logging with SparkSessionWrapper {

//  final val datalakeLoadDateFieldName = "datalake_load_date"
//  final val datalakeLoadDayFieldName = "datalake_load_day"
//  final val datalakeIngestionUuidFieldName = "datalake_ingestion_uuid"

  val statisticsSubPath = indationProperties.goldStatistics match {
      case Some(gStatistics) => gStatistics.statisticsSubPath
      case None => throw new NoSuchElementException("goldStatistics.statisticsSubPath not found in yaml config file")
    }

  val statisticsDatabase = indationProperties.goldStatistics match {
      case Some(gStatistics) => gStatistics.statisticsDatabase
      case None => throw new NoSuchElementException("goldStatistics.statisticsDatabase not found in yaml config file")
    }

  val statisticsTable = indationProperties.goldStatistics match {
      case Some(gStatistics) => gStatistics.statisticsTable
      case None => throw new NoSuchElementException("goldStatistics.statisticsTable not found in yaml config file")
    }
  
  override def receiveStatistics(activityStatistics: ActivityStatistics): Unit = {
    import com.minsait.metagold.activity.statistics.ActivityStatsJsonProtocol._
    import spark.implicits._
    import spray.json._

    this.logger.info(s"Processing statistics...")
    this.logger.info(s"statisticsSubPath: " + statisticsSubPath)
    this.logger.info(s"statisticsDatabase: " + statisticsDatabase)
    this.logger.info(s"statisticsTable: " + statisticsTable)

    // Add engine information to statistics
    val stats = activityStatistics.copy(engine = ActivityEngine(BuildInfo.name, BuildInfo.version)).toJson

    // Read statistics Json into dataFrame
    // TODO: Cuando llega vacío el array de transformaciones, genera un array de strings vacío y estropea el esquema de las estadísticas
    val activityStatisticsDF = spark
                                .read
                                .json(Seq(stats.toString()).toDS)

    // Write dataframe to destination
    writeStatistics(
      activityStatisticsDF,
      activityStatistics.uuid
    )
  }

  def unixToDateTime(timestamp: Long): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(timestamp)
    sdf.format(date)
  }

  def writeStatistics(dataframe: DataFrame,
                      uuid: String) = {

    // Add info columns
    val unix_timestamp: Long = System.currentTimeMillis
    val dataLakeStorageDate = unixToDateTime(unix_timestamp)
    val dataframeWithStorageDate =
      dataframe
        .withColumn(
          datalakeLoadDateFieldName,
          to_timestamp(
            lit(dataLakeStorageDate),
            "yyyy-MM-dd HH:mm:ss"
          ).cast("timestamp"))

    val dataframeWithStorageDay =
      dataframeWithStorageDate
        .withColumn(
          datalakeLoadDayFieldName,
          date_trunc("Day",current_timestamp())
        )

    val dataframeWithUuid =
      dataframeWithStorageDay.
        withColumn(
          datalakeIngestionUuidFieldName,
          lit(uuid)
        )

    // Write final dataframe to statistics path
    val statisticsPathToWrite = indationProperties.datalake.basePath + this.statisticsSubPath
    var attempts = 3
    var success = false
    this.logger.info(s"Writing statistics to ${statisticsPathToWrite}.")
    while (!success && attempts>0) {
      attempts -= 1
      try {
        dataframeWithUuid
          .write
          .partitionBy(datalakeLoadDayFieldName)
          .format(getOutputType(indationProperties.datalake.outputType.getOrElse(Delta)))
          .mode("APPEND")
          .save(statisticsPathToWrite)
        success=true
      }
      catch {
        case e:Throwable=>{
          if(attempts>0) {
            this.logger.warn(s"Error writing statistics to ${statisticsPathToWrite}. Pending attempts $attempts.")
          }
          else{
            this.logger.error(s"Error writing statistics to ${statisticsPathToWrite}. Pending attempts $attempts.", Some(e))
          }
        }
      }
    }
    if (!spark.catalog.databaseExists(this.statisticsDatabase)) {
      this.logger.info(s"Creating dataBase: ${this.statisticsDatabase}")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${this.statisticsDatabase}")
    }

    val tablePath = s"${this.statisticsDatabase}.${this.statisticsTable}"
    val tableExists = Try {
      spark.read.table(tablePath).take(1)
      true
    }.getOrElse(false)
    
    if (tableExists)
    {
      this.logger.info(s"Creating table: ${this.statisticsDatabase}.${this.statisticsTable}")
      spark.sql(s"CREATE TABLE IF NOT EXISTS ${this.statisticsDatabase}.${this.statisticsTable} " +
        s"USING ${getOutputType(indationProperties.datalake.outputType.getOrElse(Delta)).toUpperCase()} " +
        s"LOCATION '${statisticsPathToWrite}'")
    }
    // check parquet modifications
    if (indationProperties.datalake.outputType.get==Parquet){
      checkTableRecreation(indationProperties, this.statisticsDatabase, this.statisticsTable, statisticsPathToWrite)
      repairPartitions(indationProperties, this.statisticsDatabase +"."+ this.statisticsTable)
    }
  }


}
