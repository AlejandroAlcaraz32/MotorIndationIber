package com.minsait.metagold.activity.statistics.models

import com.minsait.metagold.metadata.models.enums.ValueToStringTypes

object ActivityResults {

  sealed case class ActivityResult(value: String) extends ValueToStringTypes

  object Success extends ActivityResult("SUCCESS")

  object Fail extends ActivityResult("FAIL")

  //TODO: Ampliar catálogo de resultados con mayor detalle

  val values = Seq(Success, Fail)
}
