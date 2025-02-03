package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.StageTypes.StageType
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode

case class StageSelect(
                        columnName: String,
                        columnDescription: String,
                        columnAlias: String
                      )