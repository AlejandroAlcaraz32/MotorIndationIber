package com.minsait.indation.silver.helper

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.metadata.models.enums.{ColumnTransformationTypes, ColumnsTypes}
import com.minsait.indation.metadata.models.{Column, ColumnTransformation}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_timestamp, when, _}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.Metadata
import scala.util.Try

object ValidationHelper extends Logging with SparkSessionWrapper {

	 def validateDFCols(df: DataFrame, cols: List[Column]): Boolean = {

		try {
			if (df.columns.length != cols.length) throw new Exception("Columns total number not match")

			var colNames: Seq[String] = Seq.empty
			for (c <- cols) {
				colNames :+= c.name
			}

			DataFrameHelpers.validatePresenceOfColumns(df, colNames)

			true
		} catch {
			case _: Exception => false
		}

	}

	def validateSchema(
    	validRowsSchematized: DataFrame, requiredSchema: StructType, datasetName: String
	): Boolean = {
		val schemaWithMetadata = requiredSchema.fields.map { x =>
			Try {
				val originMetadata = validRowsSchematized.schema(x.name).metadata
				StructField(x.name, x.dataType, x.nullable, originMetadata)
			}.getOrElse(x)
		}
		try {
			DataFrameHelpers.validateSchema(validRowsSchematized, StructType(schemaWithMetadata))
			true
		} catch {
			case ex: Exception => {
				logger.error(s"Error validating schema for dataset $datasetName",Some(ex))
				false
			}
		}
	}

	 def validateDFRows(df: DataFrame, cols: List[Column]): (DataFrame, DataFrame) = {

		val tail = "copyCheck"
		val validColName = "silvervalid"

		var dfCopy = df.withColumn(validColName, lit(true))

		for (c <- cols) {
			logger.info("Casting column " + c.name + " to " + c.typ)
			c.typ match {

				case ColumnsTypes.Integer =>
					dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("integer"))

				case ColumnsTypes.Short =>
					dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("short"))

				case ColumnsTypes.String =>
					dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("string"))

				case ColumnsTypes.Float =>
					if (c.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some(""), None)).typ == ColumnTransformationTypes.Comma) {
						dfCopy = dfCopy.withColumn(c.name + tail, (regexp_replace((regexp_replace(trim(col(c.name)), "\\.", "")), "\\,", ".")).cast("float"))
					}
					else {
						dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("float"))
					}

				case ColumnsTypes.Long =>
					dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("long"))

				case ColumnsTypes.Boolean =>
					dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("boolean"))

				case ColumnsTypes.Binary =>
					dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("binary"))

				case ColumnsTypes.Double =>
					if (c.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some(""), None)).typ == ColumnTransformationTypes.Comma) {
						dfCopy = dfCopy.withColumn(c.name + tail, (regexp_replace((regexp_replace(trim(col(c.name)), "\\.", "")), "\\,", ".")).cast("double"))
					} else {
						dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast("double"))
					}

				case ColumnsTypes.Decimal =>
					if (c.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some(""), None)).typ == ColumnTransformationTypes.Comma) {
						dfCopy = dfCopy.withColumn(c.name + tail, (regexp_replace((regexp_replace(trim(col(c.name)), "\\.", "")), "\\,", ".")).cast(DecimalType(c.decimalParameters.get.precision, c.decimalParameters.get.scale)))
					} else {
						dfCopy = dfCopy.withColumn(c.name + tail, trim(col(c.name)).cast(DecimalType(c.decimalParameters.get.precision, c.decimalParameters.get.scale)))
					}

				case ColumnsTypes.DateTime =>
					val pattern =  c.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some("dd-MM-yyyy"), None)).pattern.get
					if(pattern.endsWith(".SSSSSS")) {
						val until = pattern.indexOf(".SSSSSS")
						dfCopy = dfCopy.withColumn(c.name + tail, (unix_timestamp(substring(trim(col(c.name)),1,until+4), pattern) + substring(trim(col(c.name)),-6, 6).cast("float")/1000000).cast("timestamp"))
					}
					else {
//						spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
						dfCopy = dfCopy.withColumn(c.name + tail, to_timestamp(trim(col(c.name)), pattern).cast("timestamp"))
					}

				case ColumnsTypes.Date =>
					val pattern =  c.transformation.getOrElse(ColumnTransformation(ColumnTransformationTypes.Date, Some("dd-MM-yyyy"), None)).pattern.get
  				dfCopy = dfCopy.withColumn(c.name + tail, to_date(trim(col(c.name)), pattern).cast("date"))

				case _ => throw new UnsupportedOperationException("Unsupported Column Type " + c.typ.value)
			}

			dfCopy = dfCopy
				.withColumn(validColName, when(col(c.name + tail).isNull && col(c.name).isNotNull, false)
					.otherwise(col(validColName)))

			val dflog = dfCopy.select(c.name, c.name + tail).withColumn(validColName, when(col(c.name + tail).isNull && col(c.name).isNotNull, false)
				.otherwise(true))
			val invalidrows = dflog.filter(!col(validColName))

			if(invalidrows.count() > 0) {
				logger.warn("Error casting column " + c.name + " to " + c.typ + ". Invalid rows: " + invalidrows.count())
			}

			dfCopy = dfCopy.withColumn(c.name, col(c.name + tail)).drop(col(c.name + tail))
		}

		val validCols = dfCopy.filter(col(validColName)).drop(validColName)
		val invalidCols = dfCopy.filter(!col(validColName)).drop(validColName)

		(validCols, invalidCols)
	}

}
