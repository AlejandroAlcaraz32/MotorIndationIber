package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.ColsTypes.ColsTypes
import com.minsait.metagold.metadata.models.enums.ImputerTypes.ImputerTypes

case class StageImputer(
                            name: String,
                            description: String,
                            typ: ColsTypes,
                            strategy: Option[ImputerTypes] = Some(ImputerTypes("mean")),
                            historicalTable: Option[StageTable],
                            inputCol: Option[String],
                            inputCols: Option[Array[String]],
                            outputCol: Option[String],
                            outputCols: Option[Array[String]]
                         )
