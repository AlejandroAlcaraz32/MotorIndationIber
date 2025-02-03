package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.TransformationTypes.TransformationType

case class SQLTransformation(
                              sourceTables: List[GoldTableSource],
                              stages: List[SQLTransformationStage]
                  )
