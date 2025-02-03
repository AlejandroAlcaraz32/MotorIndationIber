package com.minsait.indation.metadata.models.enums

object PermissiveThresholdTypes {
	sealed case class PermissiveThresholdType(value: String)
	object Absolute extends PermissiveThresholdType("absolute")
	object Percentage extends PermissiveThresholdType("percentage")
}
