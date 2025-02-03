package com.minsait.indation.activity.statistics

import com.minsait.indation.activity.statistics.models.{ActivityDataset, ActivityExecution, ActivityOutputPaths, ActivitySilverPersistence}
import com.minsait.indation.datalake.writers.SilverPersistenceWriteInfo
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.IngestionTypes

import java.sql.Timestamp
import java.time.Instant

object ActivityStatisticsHelper {

  def activityExecution(start: Long, end: Long): ActivityExecution = {

    ActivityExecution(Timestamp.from(Instant.ofEpochMilli(start)), Timestamp.from(Instant.ofEpochMilli(end)), (end - start).toFloat / 1000)
  }

  def activityDataset(dataset: Dataset): ActivityDataset = {

    ActivityDataset(dataset.name, dataset.typ, dataset.version, dataset.ingestionMode, dataset.validationMode, dataset.partitionBy)
  }

  def activityOutputPaths(dataset: Option[Dataset], unknown: Option[String],
                          corrupted: Option[String],
                          schema_mismatch: Option[String],
                          invalid: Option[String],
                          bronze: Option[String],
                          silver: Option[String]): ActivityOutputPaths = {
    //val silver_historical =
    //  if (silver.isDefined && (dataset.get.ingestionMode.equals(IngestionTypes.FullSnapshot) || dataset.get.ingestionMode.equals(IngestionTypes
    //    .LastChanges)
    //    )) {
    //    Some(silver.get + "_historical")
    //  } else {
    //    None
    //  }

    ActivityOutputPaths(unknown, corrupted, schema_mismatch, invalid, bronze, silver, None)
  }

  def activitySilverPersistente(dataset: Dataset, silverPersistenceWriteInfo: SilverPersistenceWriteInfo): ActivitySilverPersistence = {

    ActivitySilverPersistence(
      Some(dataset.database),
      Some(dataset.table),
      silverPersistenceWriteInfo.principalPreviousVersion,
      silverPersistenceWriteInfo.principalCurrentVersion,
      None,
      silverPersistenceWriteInfo.historicalPreviousVersion,
      silverPersistenceWriteInfo.historicalCurrentVersion
    )
  }
}
