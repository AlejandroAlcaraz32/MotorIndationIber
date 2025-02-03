package com.minsait.metagold.activity.statistics.models

import com.minsait.metagold.metadata.models.enums.ValueToStringTypes

object ActivityTriggerTypes {

  sealed case class ActivityTriggerType(value: String) extends ValueToStringTypes

  object Adf extends ActivityTriggerType("adf")

  val values = Seq(Adf)
}
