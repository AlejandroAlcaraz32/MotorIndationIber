package com.minsait.indation.datalake.readers

import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.CsvHeaderTypes
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField}

import scala.collection.mutable
import scala.util.Try

object LandingCsvFileReader extends LandingFileReader with SparkSessionWrapper with Logging {
  override def readFile(fileInputPath: String, dataset: Dataset): Try[DataFrame] = {


    logger.info("Read pending file: " + fileInputPath)

    val schema = SchemaHelper.jsonColumnsStructTypeAllString(dataset).add(StructField("_corrupt_record", StringType, nullable = true))

    val options = mutable.Map[String, String]()
    if (dataset.fileInput.get.csv.get.escapeChar.isDefined) {
      options.put("escape", dataset.fileInput.get.csv.get.escapeChar.get)
    }

    if (dataset.fileInput.get.csv.get.lineSep.isDefined) {
      options.put("lineSep", dataset.fileInput.get.csv.get.lineSep.get)
    }

    options.put("header", (dataset.fileInput.get.csv.get.header == CsvHeaderTypes.FirstLine).toString)
    options.put("sep", dataset.fileInput.get.csv.get.delimiter)
    options.put("charset", if (dataset.fileInput.get.csv.get.charset.contains("US-ASCII")) "UTF-8" else dataset.fileInput.get.csv.get.charset)
    options.put("mode", "PERMISSIVE")
    options.put("multiline", (if (dataset.fileInput.get.csv.get.multiline.isDefined) dataset.fileInput.get.csv.get.multiline.get else false).toString)
    options.put("nullValue", if (dataset.fileInput.get.csv.get.nullValue.isDefined) dataset.fileInput.get.csv.get.nullValue.get else "")
    options.put("quote", dataset.fileInput.get.csv.get.quote.getOrElse("\""))

    Try(this.spark.read
        .options(options)
        .schema(schema)
        .csv(fileInputPath)
        .cache()
    )
  }
}
