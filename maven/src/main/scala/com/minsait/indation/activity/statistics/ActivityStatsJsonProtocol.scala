package com.minsait.indation.activity.statistics

import com.minsait.indation.activity.statistics.models.ActivityResults.ActivityResult
import com.minsait.indation.activity.statistics.models.ActivityTriggerTypes.ActivityTriggerType
import com.minsait.indation.activity.statistics.models._
import com.minsait.indation.metadata.models.enums.DatasetTypes.DatasetType
import com.minsait.indation.metadata.models.enums.IngestionTypes.IngestionType
import com.minsait.indation.metadata.models.enums.ValidationTypes.ValidationType
import spray.json.{DefaultJsonProtocol, JsString, JsValue, JsonFormat, RootJsonFormat, deserializationError}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object ActivityStatsJsonProtocol extends DefaultJsonProtocol {

  private val StringExpected = "String expected"

  private implicit object ActivityTriggerTypeFormat extends JsonFormat[ActivityTriggerType] {
    def write(m: ActivityTriggerType): JsString = JsString(m.value)

    def read(json: JsValue): ActivityTriggerType = json match {
      case JsString(s) => ActivityTriggerType(s)
      case _ => deserializationError(StringExpected)
    }
  }

  private implicit object ActivityResultFormat extends JsonFormat[ActivityResult] {
    def write(m: ActivityResult): JsString = JsString(m.value)

    def read(json: JsValue): ActivityResult = json match {
      case JsString(s) => ActivityResult(s)
      case _ => deserializationError(StringExpected)
    }
  }

  private implicit object TimestampFormat extends JsonFormat[Timestamp] {
    def write(t: Timestamp): JsString = JsString(convertTimestampToString(t))

    def read(json: JsValue): Timestamp = json match {
      case JsString(s) =>
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        val parsedDate = dateFormat.parse(s)
        new Timestamp(parsedDate.getTime)
      case _ => deserializationError(StringExpected)
    }
  }

  private implicit object ActivityIngestionFormat extends JsonFormat[IngestionType] {
    def write(m: IngestionType): JsString = JsString(m.value)

    def read(json: JsValue): IngestionType = json match {
      case JsString(s) => IngestionType(s)
      case _ => deserializationError(StringExpected)
    }
  }

  private implicit object ActivityValidationFormat extends JsonFormat[ValidationType] {
    def write(m: ValidationType): JsString = JsString(m.value)

    def read(json: JsValue): ValidationType = json match {
      case JsString(s) => ValidationType(s)
      case _ => deserializationError(StringExpected)
    }
  }

  private implicit object ActivityDatasetTypeFormat extends JsonFormat[DatasetType] {
    def write(m: DatasetType): JsString = JsString(m.value)

    def read(json: JsValue): DatasetType = json match {
      case JsString(s) => DatasetType(s)
      case _ => deserializationError(StringExpected)
    }
  }

  private def convertTimestampToString(t: Timestamp): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(t.getTime)
    sdf.format(date)
  }

  private implicit val activitySilverPersistente: JsonFormat[ActivitySilverPersistence] = jsonFormat7(ActivitySilverPersistence)
  private implicit val activityOutputPaths: JsonFormat[ActivityOutputPaths] = jsonFormat7(ActivityOutputPaths)
  private implicit val activityDataset: JsonFormat[ActivityDataset] = jsonFormat6(ActivityDataset)
  private implicit val activityEngine: JsonFormat[ActivityEngine] = jsonFormat2(ActivityEngine)
  private implicit val activityTrigger: JsonFormat[ActivityTrigger] = jsonFormat2(ActivityTrigger)
  private implicit val activityExecution: JsonFormat[ActivityExecution] = jsonFormat3(ActivityExecution)
  private implicit val activityRowsFormat: JsonFormat[ActivityRows] = jsonFormat6(ActivityRows)
  implicit val activityStatisticsFormat: RootJsonFormat[ActivityStatistics] = jsonFormat10(ActivityStatistics)
}
