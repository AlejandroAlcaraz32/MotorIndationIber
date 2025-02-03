package com.minsait.indation.metalog.helper

import com.minsait.common.utils.DateUtils
import com.minsait.indation.metalog.MetaInfo
import com.minsait.indation.metalog.MetaInfo.{Schemas, Statuses}
import com.minsait.indation.metalog.models.Ingestion

private[metalog] object IngestionLogHelper {

	private val INGESTION_CREATE = "INSERT INTO {schema}.ingestion (layer, executionStart, status) values ('{layer}', '{executionStart}', '{status}');"
	private val INGESTION_UPDATE = "UPDATE {schema}.ingestion set executionEnd='{executionEnd}', status='{status}', result='{result}', duration={duration} where ingestionId={ingestionId};"
	val primaryKey = "ingestionId"


	/**
	 * Generation of the Insert query
	 *
	 * @return
	 */
	def createInsertQuery(ingestionlog: Ingestion): String = {

		var s = INGESTION_CREATE
		s = s.replace("{schema}", Schemas.MetaLog)
			.replace("{executionStart}", DateUtils.unixToDateTime(ingestionlog.executionStart))
			.replace("{status}", Statuses.RunningState.toString)
			.replace("{layer}", ingestionlog.layer.getOrElse("").toString)
		s
	}

	/**
	 * Generation of the update query
	 *
	 * @return
	 */
	def createUpdateQuery(ingestionlog: Ingestion): String = {

		var s = INGESTION_UPDATE
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog)
			.replace("{executionEnd}", DateUtils.unixToDateTime(ingestionlog.executionEnd.get))
			.replace("{status}", MetaInfo.Statuses.FinishState.toString)
			.replace("{result}", ingestionlog.result)
			.replace("{duration}", (ingestionlog.executionEnd.get - ingestionlog.executionStart).toString)
			.replace("{ingestionId}", ingestionlog.ingestionId.toString)
		s
	}

}
