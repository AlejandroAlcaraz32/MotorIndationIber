package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.helpers.FilePatternsHelper
import com.minsait.indation.metadata.models.enums.ClassificationTypes.ClassificationType
import com.minsait.indation.metadata.models.enums.DatasetTypes
import com.minsait.indation.metadata.models.enums.DatasetTypes.DatasetType
import com.minsait.indation.metadata.models.enums.IngestionTypes.IngestionType
import com.minsait.indation.metadata.models.enums.PermissiveThresholdTypes.PermissiveThresholdType
import com.minsait.indation.metadata.models.enums.SchemaDefinitionTypes.SchemaDefinitionType
import com.minsait.indation.metadata.models.enums.ValidationTypes.ValidationType

import java.sql.Timestamp

case class Dataset(datasetId: Option[Int],
                   sourceId: Option[Int],
                   specificationId: Option[Int],
                   name: String,
                   description: String,
                   sourceName: String,
                   typ: DatasetType,
                   fileInput: Option[FileInput],
                   kafkaInput: Option[KafkaInput],
                   tableInput: Option[TableInput],
                   apiInput: Option[ApiInput],
                   version: Int,
                   enabled: Boolean,
                   effectiveDate: Option[Timestamp],
                   createDatabase: Boolean = false,
                   classification: ClassificationType,
                   partitionBy: String,
                   allowPartitionChange: Option[Boolean]=Some(true), // If false, partition values cannot be modified in Lant Changes datasets
                   ingestionMode: IngestionType,
                   database: String,
                   table: String,
                   validationMode: ValidationType,
                   permissiveThreshold: Int,
                   permissiveThresholdType: PermissiveThresholdType,
                   schemaDefinition: SchemaDefinitionType,
                   schemaColumns: Option[SchemaColumns],
                   schemaJson: Option[String],
                   qualityRules: Option[QualityRules]
                  ) {

  def isActive(before: Long): Boolean = {
    this match {
      case dataset if (dataset.enabled && dataset.effectiveDate.isEmpty) => true
      case dataset if (dataset.enabled && dataset.effectiveDate.isDefined)
                    => dataset.effectiveDate.get.getTime <= before
      case _ => false
    }
  }

  def matchesFileName(fileName: String): Boolean = {
    this.typ match {
      case DatasetTypes.File => FilePatternsHelper.fileNameMatchesFilePattern(fileName, this.fileInput.get.filePattern)
      case _ => false
    }
  }

  def getColumnByName(columnName: String): Column={
    //TODO: Falta hacer esta búsqueda para los otros tipos de definición de esquema
    val column = schemaColumns.get.columns.filter(p => p.name==columnName)
    if (column.length==1)
      column(0)
    else
      throw new UnsupportedOperationException(s"Column '$columnName' not found in dataset")

  }

  // Default value is TRUE. If partition columns must be blocked, explicit "false" value must be provided.
  def getAllowPartitionChange: Boolean={
    if (allowPartitionChange.isDefined)
      allowPartitionChange.get
    else
      true
  }

}
