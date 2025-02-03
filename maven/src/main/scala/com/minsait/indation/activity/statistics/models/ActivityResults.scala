package com.minsait.indation.activity.statistics.models

import com.minsait.indation.metadata.models.enums.ValueToStringTypes

object ActivityResults {

  sealed case class ActivityResult(value: String) extends ValueToStringTypes

  object RejectedUnknown extends ActivityResult("REJECTED_UNKNOWN")

  object RejectedCorrupted extends ActivityResult("REJECTED_CORRUPTED")

  object RejectedSchemaMismatch extends ActivityResult("REJECTED_SCHEMA_MISMATCH")

  object RejectedInvalid extends ActivityResult("REJECTED_INVALID")

  object Ingested_With_Errors extends ActivityResult("INGESTED_WITH_ERRORS")

  object Ingested_Without_Errors extends ActivityResult("INGESTED_WITHOUT_ERRORS")

  val values = Seq(RejectedCorrupted, RejectedSchemaMismatch, RejectedInvalid, Ingested_With_Errors, Ingested_Without_Errors)
}
