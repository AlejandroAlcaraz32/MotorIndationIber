package com.minsait.indation.datalake.readers

import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.FileFormatTypes
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait LandingFileReader {
  def readFile(fileInputPath: String, dataset: Dataset): Try[DataFrame]
}

object LandingFileReader {
  def apply(dataset: Dataset): LandingFileReader = dataset.fileInput.get.format match {
    case FileFormatTypes.Csv => LandingCsvFileReader
    case FileFormatTypes.Xls => LandingXlsFileReader
    case FileFormatTypes.Json => LandingJsonFileReader
    case FileFormatTypes.Orc => LandingOrcFileReader
    case FileFormatTypes.Parquet => LandingParquetFileReader
    case FileFormatTypes.Avro => LandingAvroFileReader
    case _ => throw new UnsupportedOperationException("Unsupported Dataset File Input Format " + dataset.fileInput.get.format)
  }
}
