package com.minsait.indation.metadata.validators

import com.minsait.common.configuration.models.{IndationProperties, SecurityEncryptionTypes}
import com.minsait.common.logging.Logging
import com.minsait.indation.metadata.exceptions.DatasetException
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.{ColumnsTypes, DatasetTypes, IngestionTypes}
import com.minsait.indation.silver.helper.SchemaHelper

object DatasetValidator extends Logging{
  def validateDatasetColumns(dataset: Dataset, indationProperties: IndationProperties): Unit = {
    logger.info(s"Dataset ${dataset.name} columns validation Start")
    var datasetOk = true

    //TODO: comprobar que la primary, timestamp y partitionable no sean array ni struct, ni ninguna columna interna de struct
    //TODO: Un array solo debe tener una columna como hija, es decir, solo una columna debe tenerle como parentName

    val primaryKeys = SchemaHelper.primaryKeys(dataset)
    val timestampColumn = SchemaHelper.timeStampColumn(dataset)
    val partitionableColumn = SchemaHelper.partitionableDateColumn(dataset)
    val allColumns = dataset.schemaColumns.get.columns

    val decimalColumns = allColumns.filter(_.typ == ColumnsTypes.Decimal)

    for(col <- decimalColumns){
      if (col.decimalParameters.isEmpty) {
        logger.error(s"The column ${col.name} is decimal type, so the property decimalParameters is needed", None)
        datasetOk = false
      }
    }

    if (dataset.ingestionMode == IngestionTypes.LastChanges) {
      if (primaryKeys.isEmpty) {
        logger.error("Dataset ingestion mode is last_changes, so at least one column with isPrimaryKey=true is needed", None)
        datasetOk = false
      }

      //TODO: ¿para carga incremental es necesario el isTimestamp? diría que es opcional
      if (timestampColumn.equals("")) {
        logger.error("Dataset ingestion mode is last_changes, so one column with isTimestamp=true is needed", None)
        datasetOk = false
      }
    }

    if(!timestampColumn.equals("") &&
      (dataset.ingestionMode == IngestionTypes.LastChanges || dataset.ingestionMode == IngestionTypes.Incremental)){
      if (!(allColumns.filter(_.name.equals(timestampColumn)).head.typ == ColumnsTypes.DateTime) &&
        !(allColumns.filter(_.name.equals(timestampColumn)).head.typ == ColumnsTypes.Date)) {
        logger.error("isTimestamp column must be date or datetime type", None)
        datasetOk = false
      }
    }

    if (dataset.typ == DatasetTypes.Table &&
      (dataset.ingestionMode == IngestionTypes.LastChanges || dataset.ingestionMode == IngestionTypes.Incremental)) {
      if (dataset.tableInput.get.onlyNewData.getOrElse(false)) {
        if(dataset.tableInput.get.query.isEmpty || dataset.tableInput.get.datePattern.isEmpty) {
          logger.error("Dataset ingestion mode is last_changes or incremental, and onlyNewData=true, " +
            "so next properties are mandatory: query (with isTimestampMaxSilverValue) and datePattern", None)
          datasetOk = false
        }

        if (dataset.tableInput.get.query.isDefined){
          val query = dataset.tableInput.get.query.get

          if (!query.contains("isTimestampMaxSilverValue")) {
            logger.error("Dataset ingestion mode is last_changes or incremental, and onlyNewData=true, " +
              "so query property must contain the string 'isTimestampMaxSilverValue' (without quotations) to be substituted " +
              "for the last value in the actual table", None)
            datasetOk = false
          }
        }

        if (dataset.ingestionMode == IngestionTypes.Incremental && timestampColumn.equals("")) {
          logger.error("Dataset ingestion mode is incremental with onlyNewData=true, so one column with isTimestamp=true is needed", None)
          datasetOk = false
        }
      }
    }

    if(datasetOk) {
      //Se comprueban las propiedades relacionadas con los campos sensibles y la encriptación
      if (indationProperties.security.encryption.encryptionType != SecurityEncryptionTypes.None) {
        val sensitiveColumns = SchemaHelper.sensitiveColumns(dataset)

        if (sensitiveColumns.nonEmpty) {
          if (primaryKeys.isEmpty) {
            logger.error("Encryption is enabled and there's at least a sensitive column, so one column with isPrimaryKey=true is needed", None)
            datasetOk = false
          }
          if (sensitiveColumns.contains(timestampColumn)) {
            logger.error("isTimestamp column cannot be sensitive", None)
            datasetOk = false
          }
          if (sensitiveColumns.contains(partitionableColumn)) {
            logger.error("isPartitionable column cannot be sensitive", None)
            datasetOk = false
          }
          if (dataset.qualityRules.isDefined) {
            val qualityRules = dataset.qualityRules.get

            if (qualityRules.uniquenessRule.isDefined) {
              val rulesUniqueness = qualityRules.uniquenessRule.get.rules

              rulesUniqueness.foreach(rule => {
                if (rule.columns.intersect(sensitiveColumns).nonEmpty) {
                  logger.error(s"QualityRules shouldn't have sensitive columns implied. Uniqueness rule with problems: ${rule.name}", None)
                  datasetOk = false
                }
              })
            }
            if (qualityRules.notNullRule.isDefined) {
              val nullColsToCheck = qualityRules.notNullRule.get

              if (nullColsToCheck.intersect(sensitiveColumns).nonEmpty) {
                logger.error(s"QualityRules shouldn't have sensitive columns implied. Not null rule contains sensitive column/s", None)
                datasetOk = false
              }
            }
            if (qualityRules.integrityRule.isDefined) {
              val rulesIntegrity = qualityRules.integrityRule.get.rules

              rulesIntegrity.foreach(rule => {
                if (rule.localColumns.intersect(sensitiveColumns).nonEmpty) {
                  logger.error(s"QualityRules shouldn't have sensitive columns implied. Integrity rule with problems: ${rule.name}", None)
                  datasetOk = false
                }
              })
            }
            if (qualityRules.expressionRule.isDefined) {
              val rulesExpression = qualityRules.expressionRule.get.rules

              rulesExpression.foreach(rule => {
                if (sensitiveColumns.exists(rule.expr.contains(_))) {
                  logger.error(s"QualityRules shouldn't have sensitive columns implied. Expression rule with problems: ${rule.name}", None)
                  datasetOk = false
                }
              })
            }
          }
        }
      }
    }

    if(!datasetOk) {
      logger.error(s"Dataset ${dataset.name} columns validation KO", None)
      throw new DatasetException(s"Error: There are errors at dataset ${dataset.name} configuration")
    } else {
      logger.info(s"Dataset ${dataset.name} columns validation OK")
    }
  }

}
