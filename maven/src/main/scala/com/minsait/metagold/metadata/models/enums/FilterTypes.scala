package com.minsait.metagold.metadata.models.enums

object FilterTypes {
	sealed case class FilterType(value: String)
	object FilterExpr extends FilterType("expr")

}
