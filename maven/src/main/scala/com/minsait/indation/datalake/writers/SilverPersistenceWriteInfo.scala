package com.minsait.indation.datalake.writers

case class SilverPersistenceWriteInfo(
                                      silverWritePath: String,
                                      principalPreviousVersion: Option[Long],
                                      principalCurrentVersion: Option[Long],
                                      historicalPreviousVersion: Option[Long],
                                      historicalCurrentVersion: Option[Long]
                                      )
