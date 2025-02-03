package com.minsait.metagold.metadata.models.enums

object TransformationTypes  {
	sealed case class TransformationType(value: String)
	object Datalake extends TransformationType("datalake")
	object SQL extends TransformationType("sql")
}
