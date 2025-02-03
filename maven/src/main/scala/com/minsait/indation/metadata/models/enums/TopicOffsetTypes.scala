package com.minsait.indation.metadata.models.enums

object TopicOffsetTypes {
	sealed case class TopicOffsetType(value: String) extends ValueToStringTypes
	object Earliest extends TopicOffsetType("earliest")
	object Latest extends TopicOffsetType("latest")

	val values = Seq(Earliest, Latest)
}
