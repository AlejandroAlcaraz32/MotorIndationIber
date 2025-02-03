package com.minsait.indation.bronze

import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.metadata.models.enums.{CsvHeaderTypes, PermissiveThresholdTypes, ValidationTypes}
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.metalog.models.Ingestion
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.metalog.models.enums.ReprocessTypes.ReprocessType
import com.minsait.indation.metalog.{MetaInfo, MetaLogManager}
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.joda.time.{DateTime, DateTimeZone}

class BronzeEngine(val datalakeManager: DatalakeManager, val metaLogManager: MetaLogManager)
			extends Logging with SparkSessionWrapper {

	def process(source: Source): Ingestion = {

    if (metaLogManager.existsConcurrentExecution(MetaInfo.Layers.BronzeLayerName, source.sourceId.get)) {
      logger.warn("Source " + source.name + " processed with result: Finish execution cause exists running ingestion")
      return Ingestion(layer = Some(MetaInfo.Layers.BronzeLayerName))
    }

		//Init ingestion log
		val ingestionLog = metaLogManager.initIngestionLog(MetaInfo.Layers.BronzeLayerName)
		//Init source log
		val sourceLog = metaLogManager.initSourceLog(ingestionLog.ingestionId, source.sourceId.get)

    // To read actual files for the given source.
    val pendingFilesMetadata = datalakeManager.getPendingFiles(source)

		logger.info("Number of files pending: " + pendingFilesMetadata.length)

		for (currentFile <- pendingFilesMetadata) {

			// To check if the current file belongs to one of the expected datasets.
			var fileName = currentFile.replace("\\", "/").split("/").lastOption.getOrElse("")

			val isPartialReprocess = isPartialReprocessFile(currentFile)
			if (isPartialReprocess){
				fileName = fileName.replace(".fix", "")
			}

			val matchingDatasets = source.datasets.filter(dataset =>
				dataset.isActive(DateTime.now(DateTimeZone.UTC).getMillis) && dataset.matchesFileName(fileName))

			logger.info("Process file " + currentFile)
			logger.info("Number of enabled datasets that matches with the expected file pattern: " + matchingDatasets.length)

			if (matchingDatasets.nonEmpty) {
				// If datasets are the same, it is needed to perform the rest of the validations.
				// It is assumed that only one dataset will match with the file and the dataset will only have one enabled specification.
				val dataset = matchingDatasets.head

				val datasetLog = metaLogManager.initDatasetLog(sourceLog.sourceLogId, dataset.specificationId.get)
				datasetLog.partition = Some(dataset.partitionBy)

				val dataBase = dataset.database
				val tableName = dataset.table
				val validationMode = dataset.validationMode

				val fileDate = getFileDate(fileName, dataset.fileInput.get.filePattern)

				//val fileContentDF = datalakeManager.readPending(currentFile, dataset.fileInput.csv.get, SchemaHelper.jsonColumnsStructTypeAllString(dataset))
				val fileContentDF = datalakeManager.readPending(currentFile, dataset)

        datasetLog.reprocessed = Some(setReprocessType(isPartialReprocess, fileName))

				val (isValidFile, validRowsDf, wrongRowsDf, validationMessage) =
					validate(currentFile, fileContentDF, dataset)
				logger.info("File " + currentFile + " is valid: " + isValidFile)

				if (isValidFile) {
					val rowsKO = wrongRowsDf.count()
					val rowsOK = validRowsDf.count()

					val saveBronzePath = datalakeManager.writeBronze(validRowsDf, dataBase, tableName, fileName, fileDate)

					var saveErrorPath: Option[String] = None
					if (rowsKO > 0) {
						saveErrorPath = Some(datalakeManager.writeLandingError(wrongRowsDf, dataBase, tableName, dataset.fileInput.get.csv.get, fileName, fileDate))

						datasetLog.result = MetaInfo.Result.WithErrors
						sourceLog.result = MetaInfo.Result.WithErrors
						ingestionLog.result = MetaInfo.Result.WithErrors

						if(validationMessage.isDefined) {
							datasetLog.resultMessage = validationMessage
						}
						else {
							datasetLog.resultMessage = Some(rowsKO + " rows could not be processed. validationMode: " + validationMode)
						}
					}

					datasetLog.rowsOK = Some(rowsOK)
					datasetLog.rowsKO = Some(rowsKO)
					datasetLog.outputPath = Some(saveBronzePath)
					datasetLog.errorPath = saveErrorPath
					datasetLog.pendingSilver = Some(true)

					logger.info("File " + currentFile + " processed correctly with " + rowsOK + " valid rows and " + rowsKO + " wrong rows.")
				}
				else {
					val saveErrorPath = datalakeManager.copyFileToError(currentFile, dataBase, tableName, fileName)

					datasetLog.result = MetaInfo.Result.KO
					sourceLog.result = MetaInfo.Result.WithErrors
					ingestionLog.result = MetaInfo.Result.WithErrors
					datasetLog.resultMessage = Some("Dataset could not be ingested. validationMode: " + validationMode)
					datasetLog.errorPath = Some(saveErrorPath)
					datasetLog.pendingSilver = Some(false)

					logger.info("File " + currentFile + " sent to error.")
				}

				if(datasetLog.outputPath.isDefined) {
					metaLogManager.updatePendingSilverToFalseByOutputPath(datasetLog.outputPath.get)
				}
				val saveArchivePath = datalakeManager.moveFileToLandingArchive(currentFile, dataBase, tableName, fileName)
				datasetLog.archivePath = Some(saveArchivePath)
				metaLogManager.finishDatasetLog(datasetLog)

				logger.info("File " + currentFile + " processed with result: " + datasetLog.result)
			}
		}

		metaLogManager.finishSourceLog(sourceLog)
		metaLogManager.finishIngestionLog(ingestionLog)

		logger.info("Source " + source.name + " processed with result: " + sourceLog.result)
		ingestionLog

	}

	private def validate(filePath: String, fileDF: DataFrame
											 , dataset: Dataset): (Boolean, DataFrame, DataFrame, Option[String]) = {

		import spark.implicits._

		var isValidFile = false
		var validRowsDF = spark.emptyDataFrame
		var wrongRowsDF = spark.emptyDataFrame
		var message: Option[String] = None

		val hasCsvHeaders = dataset.fileInput.get.csv.get.header == CsvHeaderTypes.FirstLine

		val actualFileSchema = datalakeManager.readPendingActualSchema(filePath, dataset.fileInput.get.csv.get)
		val expectedSchema = SchemaHelper.jsonColumnsStructTypeAllString(dataset)

		var isEqualsFieldsName = false
		if(hasCsvHeaders) {
			isEqualsFieldsName =  equalsFieldsName(actualFileSchema, expectedSchema, dataset.name)
			if(actualFileSchema.isEmpty) message = Some("Empty file")
			else if(!isEqualsFieldsName) message = Some("Schema columns names and csv header names are different.")
		}

		val inputDF =
			if(!fileDF.isEmpty && dataset.fileInput.get.csv.get.header == CsvHeaderTypes.IgnoreHeader) {
				val header = fileDF.coalesce(1).first()
				fileDF.filter(row => row != header).cache()
			}
			else {
				fileDF
			}

		if(!hasCsvHeaders || isEqualsFieldsName) {

			wrongRowsDF = inputDF
				.filter($"_corrupt_record".isNotNull)
				.select($"_corrupt_record")

			val numWrongRows = wrongRowsDF.count()

			validRowsDF = getValidRowsDF(inputDF)

			val numValidRows = validRowsDF.count()

			if (numWrongRows == 0) {
				isValidFile = true
				message = Some("Schema OK")

				if(numValidRows == 0) {
					isValidFile = false
					message = Some("Empty file")
				}
			}

			else if(dataset.validationMode == ValidationTypes.Permissive && isNumWrongRowsUnderThreshold(dataset, numWrongRows, fileDF.count())) {
				isValidFile = true
				message = Some("Schema OK with some wrong rows")

				// Because it is necessary to get the wrong rows as CSV.
				val headersStr = validRowsDF.columns.mkString(dataset.fileInput.get.csv.get.delimiter)
				wrongRowsDF = Seq(headersStr).toDF("_corrupt_record").union(wrongRowsDF)
				wrongRowsDF.cache()

				if(numValidRows == 0) {
					isValidFile = false
					message = Some("All rows in csv has different number of columns compared with schema")
				}
			}
		}
		(isValidFile, validRowsDF, wrongRowsDF, message)
	}


	private def getValidRowsDF(fileDF: DataFrame): DataFrame = {
		import spark.implicits._
		val validRowsDF = fileDF
			.filter($"_corrupt_record".isNull)
			.drop($"_corrupt_record")
		validRowsDF.cache()

		validRowsDF
	}

	private def equalsFieldsName(readSchema: StructType, expectedSchema: StructType, datasetName: String): Boolean = {

		var sameFields = false

		val readFieldsName = readSchema.fields.map(f => f.name.toLowerCase)
		val expectedFieldsName = expectedSchema.fields.map(f => f.name.toLowerCase)

		logger.info("Expected fields: " + expectedFieldsName.mkString(", "))
		logger.info("Real fields: " + readFieldsName.mkString(", "))

		if(readSchema.length == expectedSchema.length) {
			sameFields = readFieldsName.sameElements(expectedFieldsName)
			logger.info("Fields length and name comparison: " + sameFields)
		}
		else {
			logger.error(s"Schema columns and file columns mismatch in dataset $datasetName. Expected: ${expectedSchema.length}  Real: ${readSchema.length}", None)

		}

		sameFields
	}

	private def getFileDate(fileName: String, fileFormat: String): String = {

		// Example: filePattern = FILE_<yyyy><mm><dd>.csv
		val splitDatasetName = fileFormat.split("<", 2) // Split only on first occurrence.
		// datasetName = FILE_
		val datasetName = splitDatasetName(0)
		// datasetPartitions = yyyy><mm><dd>.csv
		val datasetPartitions = splitDatasetName(1).split(">(?!.*>)") // Split on last occurrence.
		// fileExtension = .csv
		val fileExtension = datasetPartitions.lastOption.getOrElse("")

		fileName
			.replace(datasetName, "")
			.replace(fileExtension, "")

	}

	private def isNumWrongRowsUnderThreshold (dataset: Dataset, numWrongRows: Double, totalColumns: Long): Boolean = {
		if(dataset.permissiveThresholdType == PermissiveThresholdTypes.Percentage){
			val percentageWrongRows = (numWrongRows*100)/totalColumns
			percentageWrongRows <= dataset.permissiveThreshold
		}
		else{
			numWrongRows <= dataset.permissiveThreshold
		}
	}

	private def isPartialReprocessFile(currentFile: String) : Boolean = {
		val splitedPath = currentFile.split("\\.")
		if (splitedPath.length > 2 && splitedPath(2) == "fix") {
			return true
		}
		false
	}

  private def setReprocessType(isPartialReprocess: Boolean, fileName: String) : ReprocessType = {
    if(isPartialReprocess){
      ReprocessTypes.Partial
    }
    else if(metaLogManager.isFullReprocess(fileName)){
      ReprocessTypes.Full
    }
    else{
      ReprocessTypes.None
    }
  }
}
