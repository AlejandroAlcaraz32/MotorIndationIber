package com.minsait.indation.bronze.validators

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.{DatasetTypes, FileFormatTypes}
import com.minsait.indation.silver.helper.{SchemaHelper, ValidationHelper}
import org.apache.spark.sql.DataFrame

trait BronzeValidator extends SparkSessionWrapper with Logging {
  def validate(indationProperties: IndationProperties,
               filePath: String,
               dataframe: DataFrame,
               dataset: Dataset,
               message: Option[String]): (Boolean, DataFrame, DataFrame)

  def validate(dataframe: DataFrame,
    dataset: Dataset): (Boolean, DataFrame, DataFrame) = {
    val requiredSchema = SchemaHelper.structTypeSchema(dataset)
    this.logger.info("Dataset required schema length " + requiredSchema.length)
    this.logger.info("Readed dataframe schema length " + dataframe.schema.length)
    val valid = dataframe.schema.length == requiredSchema.length && ValidationHelper.validateSchema(dataframe, requiredSchema,dataset.name)
    if(valid) {
      (true, dataframe, this.spark.emptyDataFrame)
    }
    else {
      (false, this.spark.emptyDataFrame, dataframe)
    }
  }
}

object BronzeValidator {
  def apply(dataset: Dataset): BronzeValidator = {
    dataset.typ match {
      case DatasetTypes.File => dataset.fileInput.get.format match {
        case FileFormatTypes.Csv => BronzeCsvValidator
        case FileFormatTypes.Json => BronzeJsonValidator
        case FileFormatTypes.Avro => BronzeFormatWithSchemaValidator
        case FileFormatTypes.Parquet => BronzeFormatWithSchemaValidator
        case FileFormatTypes.Orc => BronzeFormatWithSchemaValidator
        case FileFormatTypes.Xls => BronzeXlsValidator
        case _ => BronzeDefaultValidator
      }
      case DatasetTypes.Topic => BronzeFormatWithSchemaValidator
      case DatasetTypes.Api => BronzeJsonValidator
      case _ => BronzeFormatWithSchemaValidator
    }
  }
}
