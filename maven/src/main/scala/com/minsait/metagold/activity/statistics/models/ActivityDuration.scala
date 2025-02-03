package com.minsait.metagold.activity.statistics.models

import java.sql.Timestamp

case class ActivityDuration(
                              start: Timestamp,
                              end: Timestamp,
                              duration: Float
                            )
