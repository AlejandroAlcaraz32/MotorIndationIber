package com.minsait.indation.bronze.validators

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.{PermissiveThresholdTypes, ValidationTypes}
import org.apache.spark.sql.DataFrame

object BronzeDefaultValidator extends BronzeValidator with SparkSessionWrapper with Logging{
  override def validate(indationProperties: IndationProperties,
                        filePath: String,
                        dataframe: DataFrame,
                        dataset: Dataset,
                        message: Option[String]): (Boolean, DataFrame, DataFrame) = {

    import spark.implicits._

    var isValidFile = false
    var validRowsDF = spark.emptyDataFrame
    var wrongRowsDF = spark.emptyDataFrame

    wrongRowsDF = dataframe
      .filter($"_corrupt_record".isNotNull)
      .select($"_corrupt_record")

    val numWrongRows = wrongRowsDF.count()

    validRowsDF = getValidRowsDF(dataframe)

    val numValidRows = validRowsDF.count()

    if (numWrongRows == 0) {
      isValidFile = true
      logger.info("Schema OK")

      if (numValidRows == 0) {
        isValidFile = false
        logger.info("Empty file")
      }
    }

    else if (dataset.validationMode == ValidationTypes.Permissive && isNumWrongRowsUnderThreshold(dataset, numWrongRows, dataframe.count())) {
      isValidFile = true
      logger.warn("Schema OK with some wrong rows")

      // Because it is necessary to get the wrong rows as CSV.
      val headersStr = validRowsDF.columns.mkString(dataset.fileInput.get.csv.get.delimiter)
      wrongRowsDF = Seq(headersStr).toDF("_corrupt_record").union(wrongRowsDF)
      wrongRowsDF.cache()

      if (numValidRows == 0) {
        isValidFile = false
        logger.warn("All rows in csv has different number of columns compared with schema")
      }
    }

    (isValidFile, validRowsDF, wrongRowsDF)
  }

  def getValidRowsDF(fileDF: DataFrame): DataFrame = {
    import spark.implicits._
    val validRowsDF = fileDF
      .filter($"_corrupt_record".isNull)
      .drop($"_corrupt_record")
    validRowsDF.cache()

    validRowsDF
  }

  def isNumWrongRowsUnderThreshold (dataset: Dataset, numWrongRows: Double, totalColumns: Long): Boolean = {
    if(dataset.permissiveThresholdType == PermissiveThresholdTypes.Percentage){
      val percentageWrongRows = (numWrongRows*100)/totalColumns
      percentageWrongRows <= dataset.permissiveThreshold
    }
    else{
      numWrongRows <= dataset.permissiveThreshold
    }
  }

}
