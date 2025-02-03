package com.minsait.indation.metadata.models.enums

object ApiAuthenticationTypes {
	sealed case class ApiAuthenticationType(value: String) extends ValueToStringTypes
	object Basic extends ApiAuthenticationType("basic")
	object None extends ApiAuthenticationType("none")

	val values = Seq(Basic, None)
}
