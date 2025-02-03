package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.AggregationTypes.AggregationType
import com.minsait.metagold.metadata.models.enums.AggregationTypes.AggregationType

case class StageAggregation(
                             name: String,
                             description: String,
                             stageGroupBy: List[String],
                             aggregations: List[AggregationColumn]
                           )