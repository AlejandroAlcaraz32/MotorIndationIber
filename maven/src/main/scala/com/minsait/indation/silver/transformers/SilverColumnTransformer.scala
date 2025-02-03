package com.minsait.indation.silver.transformers

import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.{DatasetTypes, FileFormatTypes, SchemaDefinitionTypes}
import org.apache.spark.sql.DataFrame

trait SilverColumnTransformer {
  def transform(dataframe: DataFrame, dataset: Dataset): (DataFrame, DataFrame)
}

object SilverColumnTransformer {
  def apply(dataset: Dataset): SilverColumnTransformer = {
    dataset.typ match {
      case DatasetTypes.File => dataset.fileInput.get.format match {
        case FileFormatTypes.Csv | FileFormatTypes.Xls => dataset.schemaDefinition match {
          case SchemaDefinitionTypes.Columns => SilverJsonColumnsSchemaTransformer
          case _ => SilverDefaultTransformer
        }
        case _ => SilverDefaultTransformer
      }
      case _ => SilverDefaultTransformer
    }
  }
}