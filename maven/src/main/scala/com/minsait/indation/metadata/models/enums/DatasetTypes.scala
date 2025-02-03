package com.minsait.indation.metadata.models.enums

object DatasetTypes  {
	sealed case class DatasetType(value: String)
	object File extends DatasetType("file")
	object Topic extends DatasetType("topic")
	object Table extends DatasetType("table")
	object Api extends DatasetType("api")
}
