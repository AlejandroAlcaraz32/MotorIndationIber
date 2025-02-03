package com.minsait.metagold.metadata.models.enums

object AggregationTypes {
	sealed case class AggregationType(value: String)
	object Sum extends AggregationType("sum")
	object SumDistinct extends AggregationType("sumDistinct")
	object Count extends AggregationType("count")
	object CountDistinct extends AggregationType("countDistinct")
	object Min extends AggregationType("min")
	object Max extends AggregationType("max")
	object First extends AggregationType("first")
	object Last extends AggregationType("last")
	object Avg extends AggregationType("avg")
	object Mean extends AggregationType("mean")
	object StddevSamp extends AggregationType("stddev_samp")
	object StddevPop extends AggregationType("stddev_pop")
}
