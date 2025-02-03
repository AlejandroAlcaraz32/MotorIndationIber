package com.minsait.indation.metadata.models.enums

object PaginationTypes {
	sealed case class PaginationType(value: String) extends ValueToStringTypes
	object Offset extends PaginationType("offset")
	object NextLink extends PaginationType("next_link")
	object None extends PaginationType("none")

}
