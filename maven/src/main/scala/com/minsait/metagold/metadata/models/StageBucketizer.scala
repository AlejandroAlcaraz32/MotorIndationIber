package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.ColsTypes.ColsTypes

case class StageBucketizer(
                            name: String,
                            description: String,
                            typ: ColsTypes,
                            splits: Option[Array[Double]],
                            splitsArrays: Option[Array[Array[Double]]],
                            inputCol: Option[String],
                            inputCols: Option[Array[String]],
                            outputCol: Option[String],
                            outputCols: Option[Array[String]]
                         )
