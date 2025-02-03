package com.minsait.metagold.metadata.models.enums

object ColsTypes {
	sealed case class ColsTypes(value: String)
	object SingleCol extends ColsTypes("single")
	object MultipleCols extends ColsTypes("list")
}
