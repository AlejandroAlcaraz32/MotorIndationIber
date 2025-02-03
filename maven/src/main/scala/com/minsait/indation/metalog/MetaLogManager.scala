package com.minsait.indation.metalog

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.utils.DateUtils
import com.minsait.indation.metalog.MetaInfo.Layers.Layer
import com.minsait.indation.metalog.MetaInfo.Statuses
import com.minsait.indation.metalog.helper.{DatasetLogHelper, IngestionLogHelper, SourceLogHelper}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.metalog.models.{DatasetLog, Ingestion, MetaLog, SourceLog}

import java.sql.Statement
import java.text.SimpleDateFormat

class MetaLogManager extends MetaManager {
	def updatePendingSilverToFalseByOutputPath(outputPath: String): Unit = {
		this.executeUpdate(DatasetLogHelper.getPendingSilverToFalseQueryByOutputPath(outputPath))
	}

	def updatePendingSilverToFalse(dl: DatasetLog) {
		this.executeUpdate(DatasetLogHelper.getPendingSilverUpdateQuery(false, dl))
	}

	/* Constructor to initialize database connection from a ConfigurationManager. */
	def this(configManager: ConfigurationManager) {
		this()
		this.configManager = configManager
		this.dbConn = initMetadataDbConnection
	}

	/**
	 * Init for Ingestion Log
	 * @param layer Layer on which ingestion is to be performed
	 * @return
	 */
	def initIngestionLog(layer: Layer): Ingestion = {

		val ingestion = _initMetaLog(Ingestion(layer = Some(layer)))
		ingestion.asInstanceOf[Ingestion]
	}

	/**
	 * Init for Source Log
	 * @param ingestionId Id of Ingestion process Log
	 * @param sourceId    Id for source
	 * @return
	 */
	def initSourceLog(ingestionId: Int, sourceId: Int): SourceLog = {
		val source = _initMetaLog(SourceLog(ingestionId = ingestionId, sourceId = sourceId))
		source.asInstanceOf[SourceLog]
	}

	/**
	 * Init for Dataset Log
	 * @param sourceLogId     Id for the source process Log
	 * @param specificationId Id for specification
	 * @return
	 */
	def initDatasetLog(sourceLogId: Int, specificationId: Int): DatasetLog = {
		val dataset = _initMetaLog(DatasetLog(sourceLogId = sourceLogId, specificationId = specificationId))
		dataset.asInstanceOf[DatasetLog]
	}

	private def _initMetaLog(logType: Any): Any = {

		val unix_timestamp: Long = System.currentTimeMillis

		logType match {

			case ingestionLog: Ingestion =>
				val ingestion = Ingestion(
					layer = ingestionLog.layer,
					executionStart = unix_timestamp)
				val _ingestionId = insertGetId(IngestionLogHelper.createInsertQuery(ingestion), IngestionLogHelper.primaryKey)
				ingestion.ingestionId = _ingestionId
				ingestion

			case sourceLog: SourceLog =>
				val sourceLogCopy = sourceLog.copy(executionStart = unix_timestamp)
				val _sourcelogid = insertGetId(SourceLogHelper.createInsertQuery(sourceLogCopy), SourceLogHelper.primaryKey)
				sourceLogCopy.sourceLogId = _sourcelogid
				sourceLogCopy

			case datasetLog: DatasetLog =>
				val datasetLogCopy = datasetLog.copy(executionStart = unix_timestamp)
				val _datasetLogId = insertGetId(DatasetLogHelper.createInsertQuery(datasetLogCopy), DatasetLogHelper.primaryKey)
				datasetLogCopy.datasetLogId = _datasetLogId
				datasetLogCopy
		}
	}

	/**
	 * Finish Ingestion Log
	 * @param ingestion metalog object
	 * @return
	 */
	def finishIngestionLog(ingestion: Ingestion): Ingestion = {
		_finish(ingestion).asInstanceOf[Ingestion]
	}

	/**
	 * Finish Source Log
	 * @param sourceLog metalog object
	 * @return
	 */
	def finishSourceLog(sourceLog: SourceLog): SourceLog = {
		_finish(sourceLog).asInstanceOf[SourceLog]
	}

	/**
	 * Finish Dataset Log
	 * @param datasetLog metalog object
	 * @return
	 */
	def finishDatasetLog(datasetLog: DatasetLog): DatasetLog = {
		_finish(datasetLog).asInstanceOf[DatasetLog]
	}

	/**
	 * Common Finish method
	 * @param metalog     metalog object
	 * @return
	 */
	private def _finish(metalog: MetaLog): MetaLog = {

		val unix_timestamp: Long = System.currentTimeMillis

		metalog match {
			case ingestionLog: Ingestion =>
				val ingestionLogCopy = ingestionLog.copy(
					executionEnd = Some(unix_timestamp),
					status = MetaInfo.Statuses.FinishState,
					duration = Some(unix_timestamp - ingestionLog.executionStart)
				)
				executeUpdate(IngestionLogHelper.createUpdateQuery(ingestionLogCopy))
				ingestionLogCopy

			case sourceLog: SourceLog =>
				val sourceLogCopy = sourceLog.copy(
					executionEnd = Some(unix_timestamp),
					status = MetaInfo.Statuses.FinishState,
					duration = Some(unix_timestamp - sourceLog.executionStart)
				)
				executeUpdate(SourceLogHelper.createUpdateQuery(sourceLogCopy))
				sourceLogCopy

			case datasetLog: DatasetLog =>
				val datasetLogCopy = datasetLog.copy(
					executionEnd = Some(unix_timestamp),
					status = MetaInfo.Statuses.FinishState,
					duration = Some(unix_timestamp - datasetLog.executionStart)
				)
				executeUpdate(DatasetLogHelper.createUpdateQuery(datasetLogCopy))
				datasetLogCopy
		}
	}

	def getPendingSilver(SourceId: Int): List[DatasetLog] = {

		val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

		var pendingSilver: List[DatasetLog] = List()
		var s = "select d.* from {schema}.source_log s join {schema}.dataset_log d on (s.sourcelogid = d.sourcelogid) where sourceid = {sourceid} and pendingsilver = true order by d.executionEnd asc"
		s = s.replace("{schema}", MetaInfo.Schemas.MetaLog).replace("{sourceid}", SourceId.toString)
		val statement = dbConn.prepareStatement(s)
		val result = statement.executeQuery()

		while (result.next()) {
			
			var endOption: Option[Long] = None
			val endString = result.getString("executionEnd")
			if(!result.wasNull()) {
				endOption = Some(format.parse(endString).getTime)
			}

			pendingSilver = pendingSilver :+ DatasetLog(
				result.getInt("datasetLogId"),
				result.getInt("sourceLogId"),
				result.getInt("specificationId"),
				format.parse(result.getString("executionStart")).getTime,
				endOption,
				Some(result.getLong("duration")),
				Statuses.withName(result.getString("status")),
				result.getString("result"),
				Some(result.getString("resultMessage")),
				Some(result.getString("partition")),
				Some(ReprocessTypes.withName(result.getString("reprocessed"))),
				Some(result.getLong("rowsOK")),
				Some(result.getLong("rowsKO")),
				Some(result.getString("outputPath")),
				Some(result.getString("archivePath")),
				Some(result.getString("errorPath")),
				Some(result.getBoolean("pendingSilver")),
				Some(result.getInt("bronzeDatasetLogId"))
			)
		}

		pendingSilver
	}

	/**
	 * Insert a new Log instance and return the id
	 * @param sqlQueryStr query
	 * @param id_key returning name id
	 * @return
	 */
	private def insertGetId(sqlQueryStr: String, id_key: String): Int = {

		val statement = dbConn.prepareStatement(sqlQueryStr, Statement.RETURN_GENERATED_KEYS)
		statement.execute()
		val generatedKeys = statement.getGeneratedKeys
		generatedKeys.next()
		val id = generatedKeys.getInt(id_key)
		id
	}

	/**
	 * Execute an update Query
	 * @param sqlQueryStr query string
	 */
	private def executeUpdate(sqlQueryStr: String): Unit = {

		val statement = dbConn.prepareStatement(sqlQueryStr)
		statement.execute()
	}

	/**
	 * Return a class Name
	 * @param _object object to get name
	 * @return
	 */
	private def getClassName(_object: Any): String = {

		_object.getClass.getName.split("\\.").last
	}

	/**
	 * Check Layer Name
	 * @param layer layer name
	 * @return
	 */
	def validateLayer(layer: String): Boolean = {
		layer == MetaInfo.Layers.BronzeLayerName || layer == MetaInfo.Layers.SilverLayerName
	}

	private val secondsLimit = 21600
	def existsConcurrentExecution(layer: Layer, sourceId: Int): Boolean = {
		val statement = dbConn.prepareStatement(
			SourceLogHelper.createExistsExecutionQuery(
				sourceId,
				layer.toString,
				MetaInfo.Statuses.RunningState.toString,
				DateUtils.unixToDateTime(System.currentTimeMillis()),
				secondsLimit)
		)
		val result = statement.executeQuery()
		if (result.next()) {
			val count = result.getInt(1)
			if (count > 0)
				return true
		}
		false
	}

	def isFullReprocess (fileName: String): Boolean = {

		var query = """select * from metalog.dataset_log dl
            join metalog.dataset_log dlb on (dl.bronzeDatasetLogId = dlb.datasetlogid)
						join metalog.source_log sl on (dl.sourcelogid = sl.sourcelogid)
						join metalog.ingestion i on (sl.ingestionId = i.ingestionId)
						where i.layer = '{layer}' and dlb.archivePath like '%{archivePath}' and
						(dl.result = '{finished_ok}' or dl.result = '{finished_with_errors}')"""

		query = query.replace("{archivePath}", fileName)
		query = query.replace("{layer}", MetaInfo.Layers.SilverLayerName.toString)
		query = query.replace("{finished_ok}", MetaInfo.Result.OK)
		query = query.replace("{finished_with_errors}", MetaInfo.Result.WithErrors)

		val statement = dbConn.prepareStatement(query)
		val queryResult = statement.executeQuery()

		if (queryResult.next()) {
			val count = queryResult.getInt(1)
			if (count > 0)
				return true
		}
		false
	}
}