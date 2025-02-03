package com.minsait.indation.metadata.models.enums

object FileFormatTypes  {
	sealed case class FileFormatType(value: String)
	object Csv extends FileFormatType("csv")
	object Xls extends FileFormatType("xls")
	object Json extends FileFormatType("json")
	object Fixed extends FileFormatType("fixed")
	object Text extends FileFormatType("text")
	object Orc extends FileFormatType("orc")
	object Parquet extends FileFormatType("parquet")
	object Avro extends FileFormatType("avro")
}
