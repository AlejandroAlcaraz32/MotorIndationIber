package com.minsait.indation.metadata.models.enums

object JdbcAuthenticationTypes {
	sealed case class JdbcAuthenticationType(value: String) extends ValueToStringTypes
	object UserPassword extends JdbcAuthenticationType("user-password")
	object ServicePrincipal extends JdbcAuthenticationType("service-principal")

	val values = Seq(UserPassword, ServicePrincipal)
}
