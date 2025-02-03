package com.minsait.indation.metalog

object MetaInfo {

	object Layers extends Enumeration {
		type Layer = Value

		val BronzeLayerName =  Value("BRONZE")
		val SilverLayerName = Value("SILVER")
	}

	object Statuses extends Enumeration {
		type Status = Value

		val RunningState = Value("RUNNING")
		val FinishState = Value("FINISHED")
	}

	object Result {

		val OK = "FINISHED_OK"
		val KO = "FINISHED_KO"
		val WithErrors = "FINISHED_WITH_ERRORS"
	}

	private[metalog] object Schemas {
		val MetaLog = "metalog"
		val Metadata = "metadata"
	}
}
