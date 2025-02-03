package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.CalculationTypes.CalculationType
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode
import com.minsait.metagold.metadata.models.enums.CalculationTypes.CalculationType

case class StageColumn(
                        name: String,
                        description: String,
                        typ: CalculationType,
                        calculationExpr: Option[String]
                      )