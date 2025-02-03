package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.MonthlyTypes.MonthlyTypes

case class StageMonthlyConverter(
                           name: String,
                           description: String,
                           freq: MonthlyTypes, // 2 - 3 - 4 - 6
                           dateCol: String,
                           idCol: String
                         )
