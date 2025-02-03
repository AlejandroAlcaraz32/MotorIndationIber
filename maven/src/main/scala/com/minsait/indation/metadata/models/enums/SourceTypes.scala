package com.minsait.indation.metadata.models.enums

object SourceTypes  {
	sealed case class SourceType(value: String)
	object Directory extends SourceType("directory")
	object Kafka extends SourceType("kafka")
	object Jdbc extends SourceType("jdbc")
	object Api extends SourceType("api")
}
