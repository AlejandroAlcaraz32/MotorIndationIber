package com.minsait.indation.silver.transformers

import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import org.apache.spark.sql.DataFrame

object SilverDefaultTransformer extends SilverColumnTransformer with SparkSessionWrapper {
  override def transform(dataframe: DataFrame, dataset: Dataset): (DataFrame, DataFrame) =
    (dataframe, spark.emptyDataFrame)
}
