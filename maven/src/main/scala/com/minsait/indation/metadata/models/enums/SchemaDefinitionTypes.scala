package com.minsait.indation.metadata.models.enums

object SchemaDefinitionTypes {
	sealed case class SchemaDefinitionType(value: String)
	object Columns extends SchemaDefinitionType("json-columns")
	object Json extends SchemaDefinitionType("json-schema")
	object Avro extends SchemaDefinitionType("avro-schema")
}
