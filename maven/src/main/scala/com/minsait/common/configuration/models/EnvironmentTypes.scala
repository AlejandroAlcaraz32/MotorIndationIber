package com.minsait.common.configuration.models

object EnvironmentTypes {
	sealed case class EnvironmentType(value: String)
	object Local extends EnvironmentType("local")
	object Databricks extends EnvironmentType("databricks")
	object Synapse extends EnvironmentType("synapse")
	object ManagedIdentity extends EnvironmentType("managedIdentity")
}
