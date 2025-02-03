package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.AggregationTypes.AggregationType

case class AggregationColumn(
                             name: String,
                             description: String,
                             typ: AggregationType,
                             col: String
                           )