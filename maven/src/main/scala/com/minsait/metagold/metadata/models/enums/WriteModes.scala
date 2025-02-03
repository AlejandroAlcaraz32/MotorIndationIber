package com.minsait.metagold.metadata.models.enums

object WriteModes {
	sealed case class WriteMode(value: String)
	object AppendMode extends WriteMode("append")
	object OverwriteMode extends WriteMode("overwrite")
	object TruncateMode extends WriteMode("truncate")
	object ViewMode extends WriteMode("view")
}
