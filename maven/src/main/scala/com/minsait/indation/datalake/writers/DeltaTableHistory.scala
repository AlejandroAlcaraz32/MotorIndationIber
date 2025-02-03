package com.minsait.indation.datalake.writers

case class DeltaTableHistory(
                              version: Option[Long],
                              operation: Option[String],
                              readVersion: Option[Long]
                            )
