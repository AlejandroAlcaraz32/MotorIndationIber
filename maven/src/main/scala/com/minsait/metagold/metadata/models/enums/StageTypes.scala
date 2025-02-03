package com.minsait.metagold.metadata.models.enums

object StageTypes {
	sealed case class StageType(value: String)
	object Table extends StageType("table")
	object CalculatedColumn  extends StageType("calculatedColumn")
	object Select extends StageType("select")
	object Aggregation extends StageType("aggregation")
	object Filter extends StageType("filter")
	object Filler extends StageType("filler")
	object BinarizerTransform extends StageType("binarizer")
	object BucketizerTransform extends StageType("bucketizer")
	object VectorIndexerTransform extends StageType("vectorIndexer")
	object OneHotEncoderTransform extends StageType("oneHotEncoder")
	object ImputerTransform extends StageType("imputer")
	object VariationTransform extends StageType("variation")
	object MonthlyConverter extends StageType("monthlyConverter")
	object Lambda extends StageType("lambda")
}
