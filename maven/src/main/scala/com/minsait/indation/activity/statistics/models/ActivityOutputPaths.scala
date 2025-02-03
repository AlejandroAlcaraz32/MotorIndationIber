package com.minsait.indation.activity.statistics.models

case class ActivityOutputPaths(
                                unknown: Option[String],
                                corrupted: Option[String],
                                schema_mismatch: Option[String],
                                invalid: Option[String],
                                bronze: Option[String],
                                silver_principal: Option[String],
                                silver_historical: Option[String]
                              )