package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.FilterTypes.FilterType

case class StageFilter(
                        typ: FilterType,
                        filterExpr: Option[String]

                      )