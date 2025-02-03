package com.minsait.indation.metalog.helper

import com.minsait.common.utils.DateUtils
import com.minsait.indation.metalog.MetaInfo
import com.minsait.indation.metalog.MetaInfo.{Schemas, Statuses}
import com.minsait.indation.metalog.models.DatasetLog

private[metalog] object DatasetLogHelper {

	private val SOURCE_LOG_CREATE = "INSERT INTO {schema}.dataset_log (sourcelogid, specificationid, executionStart, status) values ({sourcelogid},{specificationid},'{executionStart}','{status}');"
	private val SOURCELOG_UPDATE = "UPDATE {schema}.dataset_log set executionEnd='{executionEnd}', status='{status}', result='{result}', duration={duration}{columns} where datasetlogid={datasetlogid};"
	val primaryKey = "datasetlogid"

	/**
	 * Generation of the Insert query
	 *
	 * @return
	 */
	def createInsertQuery(datasetlog: DatasetLog): String = {

		var s = SOURCE_LOG_CREATE
		s = s.replace("{schema}", Schemas.MetaLog)
			.replace("{sourcelogid}", datasetlog.sourceLogId.toString)
			.replace("{specificationid}", datasetlog.specificationId.toString)
			.replace("{executionStart}", DateUtils.unixToDateTime(datasetlog.executionStart))
			.replace("{status}", Statuses.RunningState.toString)
		s
	}

	/**
	 * Generation of the Update query
	 *
	 * @return
	 */
	def createUpdateQuery(datasetlog: DatasetLog): String = {

		var s = SOURCELOG_UPDATE
		var columns = ""
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog)
			.replace("{executionEnd}", DateUtils.unixToDateTime(datasetlog.executionEnd.get))
			.replace("{status}", MetaInfo.Statuses.FinishState.toString)
			.replace("{result}", datasetlog.result)
			.replace("{datasetlogid}", datasetlog.datasetLogId.toString)
			.replace("{duration}", (datasetlog.executionEnd.get - datasetlog.executionStart).toString)

		if (datasetlog.reprocessed.isDefined) columns += ("reprocessed='" + datasetlog.reprocessed.get + "',")
		if (datasetlog.pendingSilver.isDefined) columns += ("pendingsilver='" + datasetlog.pendingSilver.get + "',")
		if (datasetlog.resultMessage.isDefined) columns += ("resultMessage='" + datasetlog.resultMessage.get.replace("'","''") + "',")
		if (datasetlog.partition.isDefined) columns += ("partition='" + datasetlog.partition.get + "',")
		if (datasetlog.rowsOK.isDefined) columns += ("rowsOK=" + datasetlog.rowsOK.get + ",")
		if (datasetlog.rowsKO.isDefined) columns += ("rowsKO=" + datasetlog.rowsKO.get + ",")
		if (datasetlog.outputPath.isDefined) columns += ("outputPath='" + datasetlog.outputPath.get + "',")
		if (datasetlog.archivePath.isDefined) columns += ("archivePath='" + datasetlog.archivePath.get + "',")
		if (datasetlog.errorPath.isDefined) columns += ("errorPath='" + datasetlog.errorPath.get + "',")
		if (datasetlog.bronzeDatasetLogId.isDefined) columns += ("bronzeDatasetLogId=" + datasetlog.bronzeDatasetLogId.get + ",")

		if (columns != "") columns = "," + columns
		s = s.replace("{columns}", columns.dropRight(1))
		s
	}

	def getPendingSilverUpdateQuery(bool: Boolean, datasetlog: DatasetLog): String = {

		var s = "UPDATE {schema}.dataset_log set pendingsilver='{pendingSilver}' where datasetlogid={datasetlogid};"
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog)
			.replace("{datasetlogid}", datasetlog.datasetLogId.toString)
			.replace("{pendingSilver}", bool.toString)

		s
	}

	def getPendingSilverToFalseQueryByOutputPath(outputPath: String): String = {

		var s = "UPDATE {schema}.dataset_log set pendingsilver='{pendingSilver}' where outputpath='{outputpath}';"
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog)
			.replace("{outputpath}", outputPath)
			.replace("{pendingSilver}", false.toString)
		s
	}

}
