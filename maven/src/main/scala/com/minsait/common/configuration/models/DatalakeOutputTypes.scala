package com.minsait.common.configuration.models

object DatalakeOutputTypes {
	sealed case class DatalakeOutputType(value: String)
	object Delta extends DatalakeOutputType("delta")
	object Parquet extends DatalakeOutputType("parquet")
}
