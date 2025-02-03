package com.minsait.indation.silver.helper

import com.minsait.common.logging.Logging
import com.minsait.indation.silver.models.FileNamePattern
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PartitionHelper extends Logging{

	def addPartitionsByFileName(df: DataFrame, fileName: String, patterns: Array[FileNamePattern], paritionBy: String): DataFrame = {

		val file = fileName.split("/").last
		val partitions = paritionBy.split("/")
		var dfw = df

		partitions.foreach(p => {
			val pattern = patterns.find(_.element == p)
			if(pattern.isDefined) {
				val partitionValue = file.substring(pattern.get.start, pattern.get.end + 1)
				dfw = dfw.withColumn(pattern.get.element, lit(partitionValue))
			}
		})

		dfw
	}

	/**
	 * Generate columns for year, month and day based on a date column. If the columns are not used in partitioning, they will not be generated.
	 * @param df Dataframe where columns will be generated
	 * @param dateColumn Name of the date type column to be partitioned
	 * @param partitionBy Columns selected for partitioning the dataset
	 * @return
	 */
	def addPartitionsByDate(df: DataFrame, dateColumn: String, partitionBy: String): DataFrame = {

		// establece a fuego las columnas yyyy/mm/dd/hh a partir de una columna de fecha si forman parte de la partición
		var dfw = df
		val yearCol=s"${dateColumn}_yyyy"
		val monthCol=s"${dateColumn}_mm"
		val dayCol=s"${dateColumn}_dd"
		val hourCol=s"${dateColumn}_hh"
		val partitions = partitionBy.split("/")

		// year
		if (partitions.contains(yearCol))
			dfw = dfw.withColumn(yearCol, year(col(dateColumn)))
		// month
		if (partitions.contains(monthCol))
			dfw = dfw.withColumn(monthCol, month(col(dateColumn)))
		// day
		if (partitions.contains(dayCol))
			dfw = dfw.withColumn(dayCol, dayofmonth(col(dateColumn)))
		// hour
		if (partitions.contains(hourCol))
			dfw = dfw.withColumn(hourCol, hour(col(dateColumn)))

		dfw
	}

	def getFileDateFromPartitions(fileName: String, patterns: Array[FileNamePattern], partitionBy: String): String = {

		val file = fileName.split("/").last
		val partitions = partitionBy.split("/")
		var fileDate = new StringBuilder("")
		partitions.foreach(p => {
			val pattern = patterns.find(_.element == p).get
			try {
				val partitionValue = file.substring(pattern.start, pattern.end + 1)
				fileDate = fileDate.append(partitionValue)
			}
			catch {
				case ex: IndexOutOfBoundsException => this.logger.error(s"Error getting partitions from file name $fileName",Some(ex))
			}
		})

		fileDate.toString()
	}


	def getReplaceWhereFromPartitions (dataFrame: DataFrame, partitions: Array[String]): String = {
		if (partitions.length>0) {
			val partitionFields = Seq(partitions: _*)
			val distinctRows = dataFrame.select(partitionFields.head, partitionFields.tail: _*).distinct().collect()

			// TODO: Evaluar cómo se comporta con tipos de datos no STRING, ya que puede ser una fuente de problemas
			var replaceWhereStr = distinctRows.map(row=>{
				"(" + partitions.map(c=>{
					s"$c = '${row.getAs(c).toString}'"
				}).mkString(" AND ") + ")"
			}) .mkString(" OR ")

			replaceWhereStr

		}
		else
			"true" // si no hay particiones, no hay "replace where" que aplicar, por lo que reemplaza al completo (replaceWhere = true)
	}
}
