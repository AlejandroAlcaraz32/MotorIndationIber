package com.minsait.metagold.activity.statistics

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.minsait.metagold.activity.statistics.models.ActivityResults.ActivityResult
import com.minsait.metagold.activity.statistics.models._
import com.minsait.metagold.activity.statistics.models.ActivityTriggerTypes.ActivityTriggerType

import spray.json.{deserializationError, DefaultJsonProtocol, JsonFormat, JsString, JsValue, RootJsonFormat}

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

//  private implicit object ActivityIngestionFormat extends JsonFormat[ActivityStageType] {
//    def write(m: ActivityStageType): JsString = JsString(m.value)
//
//    def read(json: JsValue): ActivityStageType = json match {
//      case JsString(s) => ActivityStageType(s)
//      case _ => deserializationError(StringExpected)
//    }
//  }

  private def convertTimestampToString(t: Timestamp): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(t.getTime)
    sdf.format(date)
  }

  private implicit val activityDuration: JsonFormat[ActivityDuration] = jsonFormat3(ActivityDuration)
  private implicit val activityEngine: JsonFormat[ActivityEngine] = jsonFormat2(ActivityEngine)
  private implicit val activityParameter: JsonFormat[ActivityParameter] = jsonFormat2(ActivityParameter)
  private implicit val activityExecution: JsonFormat[ActivityExecution] = jsonFormat2(ActivityExecution)
  private implicit val activityTransformationStage: JsonFormat[ActivityTransformationStage] = jsonFormat5(ActivityTransformationStage)
  private implicit val activityTransformation: JsonFormat[ActivityTransformation] = jsonFormat7(ActivityTransformation)
  private implicit val activityTrigger: JsonFormat[ActivityTrigger] = jsonFormat2(ActivityTrigger)

  implicit val activityStatisticsFormat: RootJsonFormat[ActivityStatistics] = jsonFormat8(ActivityStatistics)
}
