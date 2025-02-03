package com.minsait.metagold.metadata.models.enums

object CalculationTypes {
	sealed case class CalculationType(value: String)
	object Expr extends CalculationType("expr")

}
