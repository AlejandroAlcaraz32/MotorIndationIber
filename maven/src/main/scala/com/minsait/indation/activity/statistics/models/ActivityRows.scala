package com.minsait.indation.activity.statistics.models

case class ActivityRows(
                         bronze_valid: Option[Long],
                         bronze_invalid: Option[Long],
                         silver_valid: Option[Long],
                         silver_invalid: Option[Long],
                         quality_valid: Option[Long],
                         quality_invalid: Option[Long]
                       )
