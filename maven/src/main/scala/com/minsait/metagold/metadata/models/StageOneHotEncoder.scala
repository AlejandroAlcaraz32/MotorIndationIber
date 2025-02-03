package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.ColsTypes.ColsTypes

case class StageOneHotEncoder(
                            name: String,
                            description: String,
                            typ: ColsTypes,
                            historicalTable: StageTable,
                            inputCol: Option[String],
                            inputCols: Option[Array[String]],
                            outputCol: Option[String],
                            outputCols: Option[Array[String]]
                         )
