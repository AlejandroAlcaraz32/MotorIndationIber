package com.minsait.indation.activity.statistics

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.BuildInfo
import com.minsait.indation.activity.statistics.models.{ActivityEngine, ActivityStatistics}
import com.minsait.indation.datalake.writers.SilverDeltaParquetTableWriter
import com.minsait.indation.metadata.MetadataReader
import com.minsait.indation.metadata.MetadataFilesManager
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.{SchemaHelper, ValidationHelper}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

class ActivityStatisticsTableWriter(override val indationProperties: IndationProperties,
                                    val metadataManager: MetadataReader)
  extends ActivityStatisticsObserver with Logging with SparkSessionWrapper {

  private val datasetStatsName = "indation"

  private val datasetPathIndation = indationProperties.silverStatistics match {
    case Some(silverStatistics) => silverStatistics.statisticsDatasetPath
    case None => throw new NoSuchElementException("StatisticsDatasetPath not found in yaml config file. For example: applications/dataset-indation/dataset-indation.json")
  }

  private val fileNameSuffix = "_indation.json"

  override def receiveStatistics(activityStatistics: ActivityStatistics): Unit = {

    import com.minsait.indation.activity.statistics.ActivityStatsJsonProtocol._
    import spark.implicits._
    import spray.json._

    val metadataReader = new MetadataFilesManager(this.indationProperties)

    val stats = activityStatistics.copy(engine = Some(ActivityEngine(BuildInfo.name, BuildInfo.version))).toJson
//    val stats = activityStatistics.copy(engine = Some(ActivityEngine(this.getClass().getPackage().getName(), this.getClass().getPackage().getSpecificationVersion()))).toJson
    this.logger.info(stats.toString())
    // val dataset = this.metadataManager.datasetByName(this.datasetStatsName)

    val dataset = metadataReader.datasetByPath(this.datasetPathIndation)

    if(dataset.isEmpty) {
      this.logger.error(s"Dataset for ingestion stats with name $datasetStatsName is not defined",None)
      return
    }

    val activityStatisticsSchema = SchemaHelper.structTypeSchema(dataset.get)

    val activityStatisticsDF = spark
                                .read
                                .schema(activityStatisticsSchema)
                                .json(Seq(stats.toString()).toDS)

    if(!ValidationHelper.validateSchema(activityStatisticsDF, activityStatisticsSchema, dataset.get.name)) {
      this.logger.error("Dataset for ingestion statistics schema mismatch", None)
      return
    }

    val timeZone = TimeZone.getTimeZone("UTC")
    val calendar = Calendar.getInstance(timeZone)
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    simpleDateFormat.setTimeZone(timeZone)
    val currentTimestamp = simpleDateFormat.format(calendar.getTime)

    SilverDeltaParquetTableWriter.writeSilver(indationProperties,
                                        currentTimestamp + this.fileNameSuffix,
                                        activityStatisticsDF,
                                        dataset.get,
                                        activityStatistics.uuid,
                                        ReprocessTypes.None)
  }
}
