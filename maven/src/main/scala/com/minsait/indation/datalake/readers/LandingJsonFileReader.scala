package com.minsait.indation.datalake.readers

import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField}

import scala.collection.mutable
import scala.util.Try

object LandingJsonFileReader extends LandingFileReader with SparkSessionWrapper {
  override def readFile(fileInputPath: String, dataset: Dataset): Try[DataFrame] = {

    val options = mutable.Map[String, String]()

    options.put("multiline", dataset.fileInput.get.json.get.multiline.toString)
    options.put("encoding", dataset.fileInput.get.json.get.encoding)

    val finalSchema = SchemaHelper.structTypeSchema(dataset).add(StructField("_corrupt_record", StringType, nullable = true))

    Try(spark.read
      .options(options)
      .schema(finalSchema)
      .json(fileInputPath)
      .cache()
    )
  }
}
