package com.minsait.metagold.activity.statistics.models

case class ActivityTransformation(
                                   name: String,
                                   typ: String,
                                   transformationResult: ActivityResults.ActivityResult,
                                   transformationDuration: ActivityDuration,
                                   transformationStages: List[ActivityTransformationStage],
                                   resultMsg: String,
                                   qualityMsg: List[String]
                                 )
