package com.minsait.metagold.metadata.models.enums

object TableTypes {
	sealed case class TableType(value: String)
	object Silver extends TableType("silver")
	object Gold extends TableType("gold")
	object SQL extends TableType("sql")
	object Test extends TableType("test")
}
