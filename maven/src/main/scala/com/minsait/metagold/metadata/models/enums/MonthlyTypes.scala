package com.minsait.metagold.metadata.models.enums

object MonthlyTypes {
	sealed case class MonthlyTypes(value: String)
	object Two extends MonthlyTypes("bi-monthly")
	object Three extends MonthlyTypes("quarterly")
	object Four extends MonthlyTypes("four-monthly")
	object Six extends MonthlyTypes("six-monthly")

}
