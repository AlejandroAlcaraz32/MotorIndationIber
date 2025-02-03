package com.minsait.indation.silver.validators

import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.QualityRulesModes.{RuleReject, RuleWarning}
import com.minsait.indation.metadata.models.enums.{PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.silver.helper.{SchemaHelper, ValidationHelper}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id}
import java.util.ArrayList
import org.apache.spark.storage.StorageLevel


object SilverValidator extends Logging with SparkSessionWrapper {
  def validate(dataset: Dataset, dataframeOK: DataFrame, dataframeKO: DataFrame, dataframeQualityKO: DataFrame): Boolean = {
    val rowsOK = dataframeOK.count()
    var rowsKO = dataframeKO.count()

    if(dataset.qualityRules.isDefined && dataset.qualityRules.get.mode == RuleReject){
      rowsKO = dataframeKO.count() + dataframeQualityKO.count()
    }
    
    val totalRows = rowsOK + rowsKO

    if (dataset.validationMode == ValidationTypes.FailFast && rowsKO > 0) {
      logger.error(s"Dataframe has corrupted records in FAIL_FAST mode on dataset ${dataset.name}", None)
      return false
    }

    if (dataset.validationMode == ValidationTypes.Permissive && !isNumWrongRowsUnderThreshold(dataset, rowsKO, totalRows)) {
      logger.error(s"Dataframe errors are greater than threshold on dataset ${dataset.name}", None)
      return false
    }

    val requiredSchema = SchemaHelper.structTypeSchema(dataset)

    ValidationHelper.validateSchema(dataframeOK, requiredSchema, dataset.name)
  }

  def validateQuality(dataset: Dataset, dataframeOk: DataFrame): (DataFrame, DataFrame) = {

    val qualityRules = dataset.qualityRules.get
    val dataframeOkwithIndex = dataframeOk.withColumn("created_index", monotonically_increasing_id).cache()
    var dataframeQualityOk: DataFrame = spark.createDataFrame(new ArrayList[Row], dataframeOkwithIndex.schema)
    var dataframeQualitykO: DataFrame = spark.createDataFrame(new ArrayList[Row], dataframeOkwithIndex.schema)

    if (qualityRules.uniquenessRule.isDefined) {
      logger.info("Checking uniqueness rules...")
      val rulesUniqueness = qualityRules.uniquenessRule.get.rules
      for (rule <- rulesUniqueness) {
        val dfNoDuplicates = dataframeOkwithIndex.dropDuplicates(rule.columns)
        val dfDuplicates = dataframeOkwithIndex.except(dfNoDuplicates)
        dataframeQualityOk = dataframeQualityOk.union(dfNoDuplicates).distinct()
        dataframeQualitykO = dataframeQualitykO.union(dfDuplicates).distinct()
        dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
        val rowsDuplicates = dfDuplicates.count()
        if (rowsDuplicates > 0 && qualityRules.mode.value == RuleWarning.value) {
          logger.warn("validateQuality Gold: Data associated with data " + dataset.name + " has "
            + rowsDuplicates + " duplicates for columns "+ rule.toString)

        }else if(rowsDuplicates > 0 && qualityRules.mode.value == RuleReject.value){
          logger.error("validateQuality Gold: Data associated with data " + dataset.name + " has "
            + rowsDuplicates + " duplicates for columns "+ rule.toString,None)
        }
      }
    }
    if (qualityRules.notNullRule.isDefined) {
      val nullColsToCheck = qualityRules.notNullRule.get
      if (nullColsToCheck.isEmpty){
        logger.info("Warning: not null rule without columns defined in metadata. Check it!")
      }
      else{
        logger.info("Checking no nulls for cols: " +nullColsToCheck.toString)
        
        // Create a column that indicates if any of the specified columns have nulls
        val nullIndicatorCol = nullColsToCheck.map(col(_).isNull).reduce(_ || _).alias("hasNulls")

        // Add the null indicator column to the dataframe
        val dfWithNullIndicator = dataframeOkwithIndex.withColumn("hasNulls", nullIndicatorCol).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // Filter rows that have any nulls in the specified columns
        val dfNulls = dfWithNullIndicator.filter(col("hasNulls")).drop("hasNulls")

        // Filter rows without any nulls in the specified columns
        val dfNoNulls = dfWithNullIndicator.filter(!col("hasNulls")).drop("hasNulls")

        dataframeQualityOk = dataframeQualityOk.union(dfNoNulls).dropDuplicates().persist(StorageLevel.MEMORY_AND_DISK_SER)
        dataframeQualitykO = dataframeQualitykO.union(dfNulls).dropDuplicates().persist(StorageLevel.MEMORY_AND_DISK_SER)
        val rowsNulls = dfNulls.count()

        logger.info("dataframeQualityOk: " + dataframeQualityOk.count())
        logger.info("dataframeQualitykO: " + dataframeQualitykO.count())     

        if (rowsNulls > 0 && qualityRules.mode.value == RuleWarning.value) {
          logger.warn("Quality SilverValidator: Data associated with " + dataset.name + " has "
            + rowsNulls + " nulls for columns "+ nullColsToCheck.toString)

        }else if(rowsNulls > 0 && qualityRules.mode.value == RuleReject.value){
          dfNulls.show()
          logger.error("Quality SilverValidator: Data associated with " + dataset.name + " has "
            + rowsNulls + " nulls for columns "+ nullColsToCheck.toString,None)
        }

        // Unpersist the cached dataframe to free up memory
        dfWithNullIndicator.unpersist()

      }
    }
    if (qualityRules.integrityRule.isDefined) {
      logger.info("Checking integrity rules...")
      val rulesIntegrity = qualityRules.integrityRule.get.rules
      for (rule <- rulesIntegrity) {
        val localColumns = rule.localColumns
        var referencedTable = spark.sql("SELECT * FROM " + rule.referencedTable)
        val referencedColumns = rule.referencedColumns
        for ((local, referenced) <- localColumns zip referencedColumns) {
          referencedTable = referencedTable.withColumnRenamed(referenced, local)
        }
        val orderedColumns = dataframeOkwithIndex.columns.map(col(_))
        val dfNoIntegrity = dataframeOkwithIndex.join(referencedTable, localColumns, "leftanti").select(orderedColumns :_*)
        val dfWithIntegrity = dataframeOkwithIndex.except(dfNoIntegrity)
        dataframeQualityOk = dataframeQualityOk.union(dfWithIntegrity).distinct()
        dataframeQualitykO = dataframeQualitykO.union(dfNoIntegrity).distinct()
        dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
        val rowsNoIntegrity = dfNoIntegrity.count()
        if (rowsNoIntegrity > 0 && qualityRules.mode.value == RuleWarning.value) {
          logger.warn("validateQuality Gold: Data associated with data " + dataset.name + " has "
            + rowsNoIntegrity + " values without integrity for columns "+ localColumns.toString)

        }else if(rowsNoIntegrity > 0 && qualityRules.mode.value == RuleReject.value){
          logger.error("validateQuality Gold: Data associated with data " + dataset.name + " has "
            + rowsNoIntegrity + " values without integrity for columns "+ localColumns.toString,None)
        }
      }
    }
    if (qualityRules.expressionRule.isDefined) {
      logger.info("Checking expression rules...")
      val rulesExpression = qualityRules.expressionRule.get.rules
      for (rule <- rulesExpression) {
        val dfValidExpr = dataframeOkwithIndex.filter(expr(rule.expr))
        val dfInvalidExpr = dataframeOkwithIndex.except(dfValidExpr)
        dataframeQualityOk = dataframeQualityOk.union(dfValidExpr).distinct()
        dataframeQualitykO = dataframeQualitykO.union(dfInvalidExpr).distinct()
        dataframeQualityOk = dataframeQualityOk.except(dataframeQualitykO)
        val rowsInvalidExpr = dfInvalidExpr.count()
        if (rowsInvalidExpr > 0 && qualityRules.mode.value == RuleWarning.value) {
          logger.warn("validateQuality Gold: Data associated with data " + dataset.name + " has "
            + rowsInvalidExpr + " rows that do not satisfy the expression "+ rule.expr)

        }else if(rowsInvalidExpr > 0 && qualityRules.mode.value == RuleReject.value){
          logger.error("validateQuality Gold: Data associated with data " + dataset.name + " has "
            + rowsInvalidExpr + " rows that do not satisfy the expression "+ rule.expr,None)
        }
      }
    }

    if (dataframeQualityOk.columns.contains("created_index"))
      dataframeQualityOk = dataframeQualityOk.drop("created_index")
    if (dataframeQualitykO.columns.contains("created_index"))
      dataframeQualitykO = dataframeQualitykO.drop("created_index")

    (dataframeQualityOk, dataframeQualitykO)
  }

  private def isNumWrongRowsUnderThreshold(dataset: Dataset, numWrongRows: Double, totalColumns: Long): Boolean = {
    if (dataset.permissiveThresholdType == PermissiveThresholdTypes.Percentage) {
      val percentageWrongRows = (numWrongRows * 100) / totalColumns
      percentageWrongRows <= dataset.permissiveThreshold
    }
    else {
      numWrongRows <= dataset.permissiveThreshold
    }
  }
}
