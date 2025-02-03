package com.minsait.indation.datalake.readers

import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import org.apache.spark.sql.DataFrame

import scala.util.Try

object LandingParquetFileReader extends LandingFileReader with SparkSessionWrapper {
  override def readFile(fileInputPath: String, dataset: Dataset): Try[DataFrame] = {

    Try(spark.read.option("spark.sql.parquet.int96RebaseModeInRead", "LEGACY").option("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .parquet(fileInputPath)
        .cache()
    )
  }
}