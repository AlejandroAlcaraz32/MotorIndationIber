package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.ColsTypes.ColsTypes

case class StageBinarizer(
                           name: String,
                           description: String,
                           typ: ColsTypes,
                           threshold: Option[Double] = Some(0.0),
                           thresholds: Option[Array[Double]],
                           inputCol: Option[String],
                           inputCols: Option[Array[String]],
                           outputCol: Option[String],
                           outputCols: Option[Array[String]]
                         )
