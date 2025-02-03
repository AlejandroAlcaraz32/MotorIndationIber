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

object LandingXlsFileReader extends LandingFileReader with SparkSessionWrapper with Logging {
  override def readFile(fileInputPath: String, dataset: Dataset): Try[DataFrame] = {
    logger.info("Read pending file: " + fileInputPath)

    val schema = SchemaHelper.jsonColumnsStructTypeAllString(dataset).add(StructField("_corrupt_record", StringType, nullable = true))

    val options = mutable.Map[String, String]()

    options.put("header", (dataset.fileInput.get.xls.get.header == CsvHeaderTypes.FirstLine).toString)
    //options.put("parseMode", "permissive")

    if (dataset.fileInput.get.xls.get.sheet.isDefined && dataset.fileInput.get.xls.get.dataRange.isDefined) {
      options.put("dataAddress", "'" + dataset.fileInput.get.xls.get.sheet.get + "'!" + dataset.fileInput.get.xls.get.dataRange.get)
    }
    else if(dataset.fileInput.get.xls.get.sheet.isDefined){
      options.put("dataAddress", "'" + dataset.fileInput.get.xls.get.sheet.get + "'!" + "A1")
    }
    else if(dataset.fileInput.get.xls.get.dataRange.isDefined) {
      options.put("dataAddress", dataset.fileInput.get.xls.get.dataRange.get)
    }

    Try(this.spark.read
        .format("excel")
        .options(options)
        .schema(schema)
        .load(fileInputPath)
        .cache()
    )
  }
}
