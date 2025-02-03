package com.minsait.metagold.metadata.models

import java.sql.Timestamp

import com.minsait.metagold.metadata.models.enums.StageTypes.StageType
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode

case class DatalakeTransformationStage(
                                        name: String,
                                        description: String,
                                        typ: StageType,
                                        tempView: Option[String],
                                        distinct: Option[Boolean],
                                        drop: Option[List[String]],
                                        cache: Option[Boolean],
                                        stageTable: Option[StageTable],
                                        stageColumn: Option[StageColumn],
                                        stageSelect: Option[List[StageSelect]],
                                        stageFilter: Option[StageFilter],
                                        stageFiller: Option[StageFiller],
                                        stageAggregation: Option[StageAggregation],
                                        stageBinarizer: Option[StageBinarizer],
                                        stageBucketizer: Option[StageBucketizer],
                                        stageVectorIndexer: Option[StageVectorIndexer],
                                        stageOneHotEncoder: Option[StageOneHotEncoder],
                                        stageImputer: Option[StageImputer],
                                        stageVariation: Option[StageVariation],
                                        stageMonthlyConverter: Option[StageMonthlyConverter],
                                        stageLambda: Option[StageLambda]
                                      )