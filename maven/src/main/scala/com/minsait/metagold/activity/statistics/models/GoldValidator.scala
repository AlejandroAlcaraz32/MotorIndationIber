package com.minsait.metagold.activity.statistics.models

import com.minsait.common.logging.Logging
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Row}
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.metagold.metadata.models.QualityRules
import com.minsait.metagold.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}

object GoldValidator extends Logging with SparkSessionWrapper {
  def validateQuality(qualityRules: QualityRules, dataframeOk: DataFrame, transformationname: String): (DataFrame, DataFrame, List[String]) = {

    val dataframeOkwithIndex = dataframeOk.withColumn("created_index", monotonically_increasing_id).cache()
    var dataframeQualityOk: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataframeOkwithIndex.schema)
    var dataframeQualitykO: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataframeOkwithIndex.schema)
    var messages: List[String] = List()

    if (qualityRules.uniquenessRule.isDefined) {
      val rulesUniqueness = qualityRules.uniquenessRule.get.rules
      for (rule <- rulesUniqueness) {
        val dfNoDuplicates = dataframeOkwithIndex.dropDuplicates(rule.columns)
        val dfDuplicates = dataframeOkwithIndex.except(dfNoDuplicates)
        dataframeQualityOk = dataframeQualityOk.union(dfNoDuplicates).distinct()
        dataframeQualitykO = dataframeQualitykO.union(dfDuplicates).distinct()
        dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
        val rowsDuplicates = dfDuplicates.count()
        if (rowsDuplicates > 0) {
          messages = messages ++ List("validateQuality Gold: Data associated with transformation " + transformationname + " has "
            + rowsDuplicates + " duplicates for columns " + rule.toString)
        }
      }
    }
    if (qualityRules.notNullRule.isDefined) {
      val nullColsToCheck = qualityRules.notNullRule.get
      val dfNoNulls = dataframeOkwithIndex.na.drop(nullColsToCheck)
      val dfNulls = dataframeOkwithIndex.except(dfNoNulls)
      dataframeQualityOk = dataframeQualityOk.union(dfNoNulls).distinct()
      dataframeQualitykO = dataframeQualitykO.union(dfNulls).distinct()
      dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
      val rowsNulls = dfNulls.count()
      if (rowsNulls > 0) {
        messages = messages ++ List("validateQuality Gold: Data associated with transformation " + transformationname + " has "
          + rowsNulls + " nulls for columns " + nullColsToCheck.toString)
      }
    }
    if (qualityRules.integrityRule.isDefined) {
      val rulesIntegrity = qualityRules.integrityRule.get.rules
      for (rule <- rulesIntegrity) {
        val localColumns = rule.localColumns
        var referencedTable = spark.sql("SELECT * FROM " + rule.referencedTable)
        val referencedColumns = rule.referencedColumns
        for ((local, referenced) <- localColumns zip referencedColumns) {
          referencedTable = referencedTable.withColumnRenamed(referenced, local)
        }
        val orderedColumns = dataframeOkwithIndex.columns.map(col(_))
        val dfNoIntegrity = dataframeOkwithIndex.join(referencedTable, localColumns, "leftanti").select(orderedColumns: _*)
        val dfWithIntegrity = dataframeOkwithIndex.except(dfNoIntegrity)
        dataframeQualityOk = dataframeQualityOk.union(dfWithIntegrity).distinct()
        dataframeQualitykO = dataframeQualitykO.union(dfNoIntegrity).distinct()
        dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
        val rowsNoIntegrity = dfNoIntegrity.count()
        if (rowsNoIntegrity > 0) {
          messages = messages ++ List("validateQuality Gold: Data associated with transformation " + transformationname + " has "
            + rowsNoIntegrity + " rows wh " + localColumns.toString)
        }
      }
    }
    if (qualityRules.expressionRule.isDefined) {
      val rulesExpression = qualityRules.expressionRule.get.rules
      for (rule <- rulesExpression) {
        val dfValidExpr = dataframeOkwithIndex.filter(expr(rule.expr))
        val dfInvalidExpr = dataframeOkwithIndex.except(dfValidExpr)
        dataframeQualityOk = dataframeQualityOk.union(dfValidExpr).distinct()
        dataframeQualitykO = dataframeQualitykO.union(dfInvalidExpr).distinct()
        dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
        val rowsInvalidExpr = dfInvalidExpr.count()
        if (rowsInvalidExpr > 0) {
          messages = messages ++ List("validateQuality Gold: Data associated with transformation " + transformationname + " has "
            + rowsInvalidExpr + " rows which do not satisfy the expression " + rule.expr)
        }
      }
    }

    if (messages.length > 0 && qualityRules.mode.value == RuleWarning.value) {
      messages.foreach(m => logger.warn(m))
    } else if (messages.length > 0 && qualityRules.mode.value == RuleReject.value) {
      messages.foreach(m => logger.error(m, None))
    }

    if (dataframeQualityOk.columns.contains("created_index"))
      dataframeQualityOk = dataframeQualityOk.drop("created_index")
    if (dataframeQualitykO.columns.contains("created_index"))
      dataframeQualitykO = dataframeQualitykO.drop("created_index")

    (dataframeQualityOk, dataframeQualitykO, messages)
  }
  /*
  def validate(dataset: Dataset, dataframeOK: DataFrame, dataframeKO: DataFrame): Boolean = {
    val rowsOK = dataframeOK.count()
    val rowsKO = dataframeKO.count()
    val totalRows =  rowsOK + rowsKO

    if(dataset.validationMode == ValidationTypes.FailFast && rowsKO > 0){
      logger.error(s"Dataframe has corrupted records in FAIL_FAST mode on dataset ${dataset.name}", None)
      return false
    }

    if (dataset.validationMode == ValidationTypes.Permissive && !isNumWrongRowsUnderThreshold(dataset, rowsKO, totalRows)){
      logger.error(s"Dataframe errors are greater than threshold on dataset ${dataset.name}", None)
      return false
    }

    val requiredSchema = SchemaHelper.structTypeSchema(dataset)
    //    ValidationHelper.validateSchema(dataframeOK, requiredSchema) && rowsOK > 0 //TODO: Confirmar que cargar 0 registros se considera correcto. Si el orgien está bien formado, pero tiene 0 registros, no hay por qué generar error.

    ValidationHelper.validateSchema(dataframeOK, requiredSchema, dataset.name)
  }
*/

  /*  private def isNumWrongRowsUnderThreshold (dataset: Dataset, numWrongRows: Double, totalColumns: Long): Boolean = {
      if(dataset.permissiveThresholdType == PermissiveThresholdTypes.Percentage){
        val percentageWrongRows = (numWrongRows*100)/totalColumns
        percentageWrongRows <= dataset.permissiveThreshold
      }
      else{
        numWrongRows <= dataset.permissiveThreshold
      }
    }*/
}
