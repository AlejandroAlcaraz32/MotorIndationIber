package com.minsait.common.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  protected lazy val spark: SparkSession = SparkSessionFactory.getSparkSession
}
