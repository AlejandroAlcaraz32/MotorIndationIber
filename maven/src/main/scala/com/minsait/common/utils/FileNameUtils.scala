package com.minsait.common.utils

import com.minsait.common.configuration.compression.FileFormatCompressionConfigurations
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.{DatasetTypes, FileFormatTypes}
import com.minsait.indation.silver.models.FileNamePattern

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, TimeZone}

object FileNameUtils {

//	val GoldDirectory = "gold"
//	val SilverDirectory = "silver"

	def patterns(s: String): Array[FileNamePattern] = {

		var result: Array[FileNamePattern] = Array.empty[FileNamePattern]
		val p = Pattern.compile("<(.*?)>")
		val m = p.matcher(s)
		var matchings = 0
		while (m.find()) {
			matchings = matchings + 1
			val sf = m.toMatchResult
			val aux = m.group(1)
			result = result :+ FileNamePattern(aux, sf.start() - ((matchings - 1) * 2), sf.end() - (2 * matchings + 1))
		}
		result
	}

	def fileNameWithCompression(fileName: String): String = {
		if(fileName.endsWith(".csv.gz")) {
			return fileName.replace(".gz", "") + "." +
				FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Csv).extension
		}

		val extension = fileName.split("\\.").last
		FileFormatTypes.FileFormatType(extension) match  {
			case FileFormatTypes.Csv => fileName + "." +
				FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Csv).extension
			case FileFormatTypes.Json => fileName + "." +
				FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Json).extension
			case FileFormatTypes.Text => fileName + "." +
				FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Text).extension
			case FileFormatTypes.Fixed =>
				fileName + "." + FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Fixed).extension
			case FileFormatTypes.Orc =>
				fileName.take(1 + fileName.lastIndexOf(".") )+
					FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Orc).extension + "." + extension
			case FileFormatTypes.Parquet =>
				fileName.take(1 + fileName.lastIndexOf(".") )+
					FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Parquet).extension + "." + extension
			case FileFormatTypes.Avro =>
				fileName.take(1 + fileName.lastIndexOf(".") )+
					FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Avro).extension + "." + extension
			case _ =>  fileName + "." + FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Text).extension
		}
	}

	def landingDestinationPath(fileName: String, basePath: String, sublayer: String, ds: Dataset): String = {
		val destinationPath = basePath + sublayer + "/" + ds.sourceName + "/" + ds.name + "/"

		this.destinationSubdirsPath(fileName, ds, destinationPath)
	}

	def destinationSubdirsPath(fileName: String, ds: Dataset, destinationPath: String): String = {
		ds.typ match {
			case DatasetTypes.File => val patternFields = FileNameUtils.patterns(ds.fileInput.get.filePattern)
				val subdirs = patternFields.map(pattern => {
					fileName.substring(pattern.start, pattern.end + 1)
				})
				destinationPath + subdirs.mkString("/") + "/"
			case DatasetTypes.Topic => destinationPath + this.nowSimpleDateFormat("yyyy/MM/dd/")
			case DatasetTypes.Api => destinationPath + this.nowSimpleDateFormat("yyyy/MM/dd/")
			case _ => destinationPath + this.nowSimpleDateFormat("yyyy/MM/dd/")
		}
	}

	def fileExtension(fileName: String): String = {
		val splitedFileName = fileName.split("/").last.split("\\.")
		if (splitedFileName.length > 2) {
			"." + splitedFileName.dropRight(1).last + "." + splitedFileName.last
		}
		else {
			"." + splitedFileName.last
		}
	}

	def addTimestampToFileName(fileName: String, timestamp: String): String = {
		val fileExtension = this.fileExtension(fileName)
		fileName.replace(fileExtension, "") + "_v" + timestamp + fileExtension
	}

	def avroStreamingFileName(dataset: Dataset): String = {
		val currentTimestamp =  this.nowSimpleDateFormat("yyyyMMdd_yyyyMMddHHmmss")
		val splittedTimeStamp = currentTimestamp.split("_")

		s"${splittedTimeStamp(0)}_d_${dataset.name}_${splittedTimeStamp(1)}_I.bz2.avro"
	}

	def getBronzeFileNameForTable(dataset: Dataset):String={
		s"${
			if(dataset.tableInput.get.schema.isDefined)
				s"${dataset.tableInput.get.schema.get}_${dataset.tableInput.get.table}"
			else dataset.tableInput.get.table
		}_${this.nowSimpleDateFormat("yyyyMMdd_HHmmss")}.csv"
	}

	private def nowSimpleDateFormat(format: String): String  = {
		val timeZone = TimeZone.getTimeZone("UTC")
//		val timeZone = TimeZone.getDefault //TODO: Validar si es preferible la zona local o UTC
		val calendar = Calendar.getInstance(timeZone)
		val simpleDateFormat = new SimpleDateFormat(format)
		simpleDateFormat.setTimeZone(timeZone)
		simpleDateFormat.format(calendar.getTime)
	}
}
