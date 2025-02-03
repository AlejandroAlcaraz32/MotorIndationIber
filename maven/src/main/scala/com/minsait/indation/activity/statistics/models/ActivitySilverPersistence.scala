package com.minsait.indation.activity.statistics.models

case class ActivitySilverPersistence(
                                    database: Option[String],
                                    principal_table: Option[String],
                                    principal_previous_version: Option[Long],
                                    principal_current_version: Option[Long],
                                    historical_table: Option[String],
                                    historical_previous_version: Option[Long],
                                    historical_current_version: Option[Long]
                                    )
