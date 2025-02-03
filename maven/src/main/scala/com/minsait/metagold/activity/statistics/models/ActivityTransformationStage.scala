package com.minsait.metagold.activity.statistics.models

//import com.minsait.metagold.activity.statistics.models.ActivityStageTypes.ActivityStageType

case class ActivityTransformationStage(
                            name: String,
                            typ: String,
                            result: ActivityResults.ActivityResult,
                            rows: Long,
                            duration: ActivityDuration
                          )
