package com.minsait.indation.bronze.validators

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import org.apache.spark.sql.DataFrame

object BronzeFormatWithSchemaValidator extends BronzeValidator with SparkSessionWrapper with Logging {
  override def validate(indationProperties: IndationProperties,
                        filePath: String,
                        dataframe: DataFrame,
                        dataset: Dataset,
                        message: Option[String]): (Boolean, DataFrame, DataFrame) = {

    this.validate(dataframe, dataset)
  }
}
