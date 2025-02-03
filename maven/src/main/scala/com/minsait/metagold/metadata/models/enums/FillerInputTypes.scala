package com.minsait.metagold.metadata.models.enums

object FillerInputTypes {
	sealed case class FillerInputTypes(value: String)
	object Stype extends FillerInputTypes("string")
	object Dtype extends FillerInputTypes("double")
	object Itype extends FillerInputTypes("int")
	object Btype extends FillerInputTypes("boolean")
	object Ltype extends FillerInputTypes("long")

}
