package com.minsait.indation.activity.statistics.models

import com.minsait.indation.metadata.models.enums.ValueToStringTypes

object ActivityTriggerTypes {

  sealed case class ActivityTriggerType(value: String) extends ValueToStringTypes

  object Adf extends ActivityTriggerType("adf")

  val values = Seq(Adf)
}
