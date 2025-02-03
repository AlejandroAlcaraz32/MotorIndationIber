package com.minsait.indation.bronze.validators

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.CsvHeaderTypes.{FirstLine, IgnoreHeader}
import com.minsait.indation.metadata.models.enums.ValidationTypes
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object BronzeXlsValidator extends BronzeValidator with SparkSessionWrapper with Logging {
  override def validate(indationProperties: IndationProperties,
                        filePath: String,
                        dataframe: DataFrame,
                        dataset: Dataset,
                        message: Option[String]): (Boolean, DataFrame, DataFrame) = {

    import spark.implicits._

    var isValidFile = false
    var validRowsDF = spark.emptyDataFrame
    var wrongRowsDF = spark.emptyDataFrame

    val datalakeManager = new DatalakeManager(indationProperties)

    val hasXlsxHeaders = dataset.fileInput.get.xls.get.header == FirstLine

    val actualFileSchema = datalakeManager.readPendingActualSchemaXls(filePath, dataset.fileInput.get.xls.get)
    val expectedSchema = SchemaHelper.jsonColumnsStructTypeAllString(dataset)

    var isEqualsFieldsName = false
    if(hasXlsxHeaders) {
      isEqualsFieldsName =  equalsFieldsName(actualFileSchema, expectedSchema, dataset.name)
      if(actualFileSchema.isEmpty) logger.info("Empty file")
      else if(!isEqualsFieldsName) logger.info("Schema columns names and xls header names are different.")
    }

    val inputDF =
      if(!dataframe.isEmpty && dataset.fileInput.get.xls.get.header == IgnoreHeader) {
        val header = dataframe.coalesce(1).first()
        dataframe.filter(row => row != header).cache()
      }
      else {
        dataframe
      }

    if(!hasXlsxHeaders || isEqualsFieldsName) {

      wrongRowsDF = inputDF
        .filter($"_corrupt_record".isNotNull)
        .select($"_corrupt_record")

      val numWrongRows = wrongRowsDF.count()

      validRowsDF = BronzeDefaultValidator.getValidRowsDF(inputDF)

      val numValidRows = validRowsDF.count()

      if (numWrongRows == 0) {
        isValidFile = true
        logger.info("Schema OK")

        if(numValidRows == 0) {
          isValidFile = false
          logger.info("Empty file")
        }
      }
      else if(dataset.validationMode == ValidationTypes.Permissive && BronzeDefaultValidator.isNumWrongRowsUnderThreshold(dataset, numWrongRows, dataframe.count())) {
        isValidFile = true
        logger.warn("Schema OK with some wrong rows")

        if(numValidRows == 0) {
          isValidFile = false
          logger.warn("All rows in xls has different number of columns compared with schema")
        }
      }
    }

    (isValidFile, validRowsDF, wrongRowsDF)
  }

  private def equalsFieldsName(readSchema: StructType, expectedSchema: StructType, datasetName: String): Boolean = {

    var sameFields = false

    val readFieldsName = readSchema.fields.map(f => f.name.toLowerCase)
    val expectedFieldsName = expectedSchema.fields.map(f => f.name.toLowerCase)

    logger.info("Expected fields: " + expectedFieldsName.mkString(", "))
    logger.info("Real fields: " + readFieldsName.mkString(", "))

    if(readSchema.length == expectedSchema.length) {
      sameFields = readFieldsName.sameElements(expectedFieldsName)
      logger.info("Fields length and name comparison: " + sameFields)
    }
    else {
      logger.error(s"Schema columns and file columns mismatch in dataset $datasetName. Expected: ${expectedSchema.length}  Real: ${readSchema.length}", None)
    }

    sameFields
  }
}
