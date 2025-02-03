package com.minsait.metagold.metadata.models.enums

object ActivityTypes {
	sealed case class ActivityType(value: String)
	object Concurrent extends ActivityType("concurrent")
	object Parallel extends ActivityType("parallel")
}
