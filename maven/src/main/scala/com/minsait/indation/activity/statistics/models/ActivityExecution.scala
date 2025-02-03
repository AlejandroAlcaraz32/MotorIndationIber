package com.minsait.indation.activity.statistics.models

import java.sql.Timestamp

case class ActivityExecution(
                              start: Timestamp,
                              end: Timestamp,
                              duration: Float
                            )
