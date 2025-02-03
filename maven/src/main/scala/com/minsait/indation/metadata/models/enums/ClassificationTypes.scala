package com.minsait.indation.metadata.models.enums

object ClassificationTypes {
	sealed case class ClassificationType(value: String) extends ValueToStringTypes
	object Public extends ClassificationType("public")
	object Private extends ClassificationType("private")

	val values = Seq(Public, Private)
}
