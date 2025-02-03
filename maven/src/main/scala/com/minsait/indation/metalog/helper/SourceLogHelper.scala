package com.minsait.indation.metalog.helper

import com.minsait.common.utils.DateUtils
import com.minsait.indation.metalog.MetaInfo
import com.minsait.indation.metalog.MetaInfo.{Schemas, Statuses}
import com.minsait.indation.metalog.models.SourceLog

private[metalog] object SourceLogHelper {

	private val SOURCE_LOG_CREATE = "INSERT INTO {schema}.source_log (ingestionid, sourceid, executionStart, status) values ({ingestionid},{sourceid},'{executionStart}','{status}');"
	private val SOURCELOG_UPDATE = "UPDATE {schema}.source_log set executionEnd='{executionEnd}', status='{status}', result='{result}', duration={duration} where sourcelogid={sourcelogid};"
	private val SOURCE_LOG_EXISTS_EXECUTION =
		"""
     select count(*) from {schema}.source_log sl
       		inner join {schema}.ingestion i on (sl.ingestionId = i.ingestionId)
       		where sl.sourceid={sourceid} and i.layer='{layer}' and sl.status='{status}' and extract(epoch from ('{currentTime}'::timestamp - sl.executionStart ))<{secondsLimit};
    """
	val primaryKey = "sourceLogId"

	/**
	 * Generation of the Insert query
	 * @return
	 */
	def createInsertQuery(sourcelog: SourceLog): String = {

		var s = SOURCE_LOG_CREATE
		s = s.replace("{schema}", Schemas.MetaLog)
			.replace("{ingestionid}", sourcelog.ingestionId.toString)
			.replace("{sourceid}", sourcelog.sourceId.toString)
			.replace("{executionStart}", DateUtils.unixToDateTime(sourcelog.executionStart))
			.replace("{status}", Statuses.RunningState.toString)
		s
	}

	/**
	 * Generation of the Update query
	 * @return
	 */
	def createUpdateQuery(sourcelog: SourceLog): String = {

		var s = SOURCELOG_UPDATE
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog)
			.replace("{executionEnd}", DateUtils.unixToDateTime(sourcelog.executionEnd.get))
			.replace("{status}", MetaInfo.Statuses.FinishState.toString)
			.replace("{result}", sourcelog.result)
			.replace("{sourcelogid}", sourcelog.sourceLogId.toString)
			.replace("{duration}", (sourcelog.executionEnd.get - sourcelog.executionStart).toString)
		s
	}

	def createExistsExecutionQuery(sourceId: Int, layer: String, status: String, currentTime: String, secondsLimit: Int): String = {
		var s = this.SOURCE_LOG_EXISTS_EXECUTION
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog)
			.replace("{sourceid}", sourceId.toString)
			.replace("{layer}", layer)
			.replace("{status}", status)
			.replace("{currentTime}", currentTime)
			.replace("{secondsLimit}", secondsLimit.toString)
		s
	}
}
