package com.minsait.metagold.metadata.models.enums

object ImputerTypes {
	sealed case class ImputerTypes(value: String)
	object mean extends ImputerTypes("mean")
	object median extends ImputerTypes("median")
	object mode extends ImputerTypes("mode")

}
