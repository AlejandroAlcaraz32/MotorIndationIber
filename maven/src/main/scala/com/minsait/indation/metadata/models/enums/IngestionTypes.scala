package com.minsait.indation.metadata.models.enums

object IngestionTypes {
	sealed case class IngestionType(value: String)
	object FullSnapshot extends IngestionType("full_snapshot")
	object Incremental extends IngestionType("incremental")
	object LastChanges extends IngestionType("last_changes")
}
