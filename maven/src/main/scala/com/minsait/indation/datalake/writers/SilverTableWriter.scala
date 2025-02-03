package com.minsait.indation.datalake.writers

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.utils.{DateUtils, FileNameUtils, SilverConstants}
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.DatasetTypes
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.{PartitionHelper, SchemaHelper}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, date_trunc, lit, to_timestamp}

trait SilverTableWriter extends Logging {

  protected def addStorageInfoColumns(fileInputPath: String, dataframe: DataFrame, dataset: Dataset, uuid: String): DataFrame = {
    val unix_timestamp: Long = System.currentTimeMillis
    val dataLakeStorageDate = DateUtils.unixToDateTime(unix_timestamp)

    val validRowsWithStorageDate =
      dataframe
        .withColumn(
          SilverConstants.datalakeLoadDateFieldName,
          to_timestamp(
            lit(dataLakeStorageDate),
            "yyyy-MM-dd HH:mm:ss"
          ).cast("timestamp"))

    val validRowsWithStorageDay =
      validRowsWithStorageDate
        .withColumn(
          SilverConstants.datalakeLoadDayFieldName,
          date_trunc("Day",current_timestamp())
        )

    val validRowsWithIndationUuid =
      validRowsWithStorageDay.
        withColumn(
          SilverConstants.datalakeIngestionUuidFieldName,
          lit(uuid)
        )

    val partitionableDateColumn = SchemaHelper.partitionableDateColumn(dataset)
    val partitionableDateColumnFinal = SchemaHelper.getColumnAlias(dataset, partitionableDateColumn).getOrElse(partitionableDateColumn)

    val dfToWriteSilver = if(dataset.typ.equals(DatasetTypes.File) && partitionableDateColumnFinal=="") {
      this.logger.info("Getting partitions info")
      val partitionPatterns = FileNameUtils.patterns(dataset.fileInput.get.filePattern)

      logger.info("Adding partitions to dataframe")
       PartitionHelper.addPartitionsByFileName(validRowsWithIndationUuid, fileInputPath, partitionPatterns, dataset.partitionBy)
    }
    else if((dataset.typ.equals(DatasetTypes.Table) || dataset.typ.equals(DatasetTypes.Api) || dataset.typ.equals(DatasetTypes.File)) && partitionableDateColumnFinal!="") {
      logger.info("Adding date partitions to table dataframe")
        PartitionHelper.addPartitionsByDate(validRowsWithIndationUuid, partitionableDateColumnFinal, dataset.partitionBy)
    }
    else if(dataset.typ.equals(DatasetTypes.Topic)){
      val topicPartitionBy =
        s"${SilverConstants.datalakeLoadDateFieldName}_yyyy/" +
        s"${SilverConstants.datalakeLoadDateFieldName}_mm/" +
        s"${SilverConstants.datalakeLoadDateFieldName}_dd/" +
        s"${SilverConstants.datalakeLoadDateFieldName}_hh"
      logger.info(s"Adding default streaming partition columns: $topicPartitionBy")
      PartitionHelper.addPartitionsByDate(validRowsWithIndationUuid, SilverConstants.datalakeLoadDateFieldName, topicPartitionBy)
    }
    else {
      validRowsWithIndationUuid
    }

    dfToWriteSilver
  }

  def writeSilver(indationProperties: IndationProperties,
                  fileInputPath: String,
                  dataframe: DataFrame,
                  dataset: Dataset,
                  uuid: String,
                  reprocessType: ReprocessTypes.ReprocessType): (SilverPersistenceWriteInfo, Option[SilverPersistenceWriteInfo])

}
