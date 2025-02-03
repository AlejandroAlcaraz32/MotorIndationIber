package com.minsait.indation.metadata.models.enums

object ColumnsTypes {
	sealed case class ColumnsType(value: String) extends ValueToStringTypes
	object Integer extends ColumnsType("integer")
	object Short extends ColumnsType("short")
	object String extends ColumnsType("string")
	object Float extends ColumnsType("float")
	object Long extends ColumnsType("long")
	object Boolean extends ColumnsType("boolean")
	object Double extends ColumnsType("double")
	object Decimal extends ColumnsType("decimal")
	object Date extends ColumnsType("date")
	object DateTime extends ColumnsType("datetime")
	object Binary extends ColumnsType("binary")
	object Array extends ColumnsType("array")
	object Struct extends ColumnsType("struct")


	val values = Seq(Integer, Short, String, Float, Long, Boolean, Double, Decimal, DateTime, Binary, Array, Struct)
}
