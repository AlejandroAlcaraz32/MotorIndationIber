package com.minsait.indation.metadata.models.enums

object CsvHeaderTypes  {
	sealed case class CsvHeaderType(value: String) extends ValueToStringTypes
	object FirstLine extends CsvHeaderType("first_line")
	object WithoutHeader extends CsvHeaderType("without_header")
	object IgnoreHeader extends CsvHeaderType("ignore_header")

	val values = Seq(FirstLine, WithoutHeader, IgnoreHeader)
}
