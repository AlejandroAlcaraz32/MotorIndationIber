package com.minsait.indation.silver.transformers

import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.enums.{ColumnTransformationTypes, ColumnsTypes}
import com.minsait.indation.metadata.models.{Column, ColumnTransformation, Dataset}
import com.minsait.indation.silver.helper.ValidationHelper.logger
import com.minsait.indation.silver.udf.SilverUdfCatalog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

object SilverJsonColumnsSchemaTransformer extends SilverColumnTransformer with SparkSessionWrapper {
  private val tail = "copyCheck"
  private val validColName = "silvervalid"

  override def transform(dataframe: DataFrame, dataset: Dataset): (DataFrame, DataFrame) = {
    var dfCopy = dataframe.withColumn(validColName, lit(true))

    for (c <- dataset.schemaColumns.get.columns) {

      logger.info("Casting column " + c.name + " to " + c.typ)
      dfCopy = this.applyTransformation(c, dfCopy)

      if(!c.typ.equals(ColumnsTypes.String)) {
        dfCopy = this.applyCast(c, dfCopy)

        dfCopy = dfCopy
          .withColumn(validColName, when(col(c.name + tail).isNull && col(c.name).isNotNull, false)
            .otherwise(col(validColName)))

        val dflog = dfCopy.select(c.name, c.name + tail).withColumn(validColName, when(col(c.name + tail).isNull && col(c.name).isNotNull, false)
          .otherwise(true))
        val invalidrows = dflog.filter(!col(validColName))

        val invalidRowsCount = invalidrows.count()
        if(invalidRowsCount > 0) {
          logger.warn("Error casting column " + c.name + " to " + c.typ + ". Invalid rows: " + invalidRowsCount)
        }

        dfCopy = dfCopy.withColumn(c.name, col(c.name + tail)).drop(col(c.name + tail))
      }
    }

    val validCols = dfCopy.filter(col(validColName)).drop(validColName)
    val invalidCols = dfCopy.filter(!col(validColName)).drop(validColName)

    (validCols, invalidCols)
  }

  private def applyTransformation(column: Column, dataframe: DataFrame): DataFrame = {
    if(column.transformation.isDefined) {
      column.transformation.get.typ match {
        case ColumnTransformationTypes.Comma =>
          dataframe.withColumn(column.name, regexp_replace(regexp_replace(trim(col(column.name)), "\\.", ""), "\\,", "."))
        case ColumnTransformationTypes.CommaEng =>
          dataframe.withColumn(column.name, regexp_replace(trim(col(column.name)), "\\,", ""))
        case ColumnTransformationTypes.Udf =>
          val udfTransformation = udf(SilverUdfCatalog.udfCatalog.filter(t =>
            t._1.equals(column.transformation.get.udf.get)).map(t => t._2).head)
          dataframe.withColumn(column.name, udfTransformation(trim(col(column.name))))
      case _ => dataframe.withColumn(column.name, trim(col(column.name)))
      }
    } else {
      dataframe.withColumn(column.name, trim(col(column.name)))
    }
  }

  private def applyCast(column: Column, dataframe: DataFrame): DataFrame = {
    column.typ match {

      case ColumnsTypes.Integer =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("integer"))

      case ColumnsTypes.Short =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("short"))

      case ColumnsTypes.Float =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("float"))

      case ColumnsTypes.Long =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("long"))

      case ColumnsTypes.Boolean =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("boolean"))

      case ColumnsTypes.Double =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("double"))

      case ColumnsTypes.Binary =>
        dataframe.withColumn(column.name + tail, col(column.name).cast("binary"))

      case ColumnsTypes.Decimal =>
        dataframe.withColumn(column.name + tail, col(column.name).cast(DecimalType(column.decimalParameters.get.precision, column.decimalParameters.get.scale)))

      case ColumnsTypes.Date =>
        val pattern =  column.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some("dd-MM-yyyy"), None)).pattern.get
        dataframe.withColumn(column.name + tail, to_date(trim( regexp_replace(col(column.name),"0001","1900")), pattern).cast("date"))

      case ColumnsTypes.DateTime =>
        val pattern =  column.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some("dd-MM-yyyy"), None)).pattern.get
        if(pattern.endsWith(".SSSSSS")) {
          val until = pattern.indexOf(".SSSSSS")
          
          val regex = "(0[0-9]{3}|1[0-8][0-9]{2})"
          dataframe.withColumn(column.name + tail, (unix_timestamp(substring(regexp_replace(col(column.name),regex,"1900"),1,until + 4), pattern) +
            substring(col(column.name),-6, 6).cast("float")/1000000).cast("timestamp"))
        }
        else {
          //          spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

          val regex = "(0[0-9]{3}|1[0-8][0-9]{2})"
          dataframe.withColumn(column.name + tail, to_timestamp(trim( regexp_replace(col(column.name),regex,"1900")), pattern).cast("timestamp"))
        }

      case _ => throw new UnsupportedOperationException("Unsupported Column Type " + column.typ.value)
    }
  }
}
