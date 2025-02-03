package com.minsait.indation.metadata.models.enums

object ValidationTypes {
	sealed case class ValidationType(value: String)
	object FailFast extends ValidationType("fail_fast")
	object Permissive extends ValidationType("permissive")
}
