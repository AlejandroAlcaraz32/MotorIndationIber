package com.minsait.indation.metalog.models

import com.minsait.indation.metalog.MetaInfo.Statuses.Status
import com.minsait.indation.metalog.MetaInfo.{Result, Statuses}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.metalog.models.enums.ReprocessTypes.ReprocessType

case class DatasetLog(var datasetLogId: Int = 0,
                      var sourceLogId: Int = 0,
                      var specificationId: Int = 0,
                      var executionStart: Long = 0,
                      var executionEnd: Option[Long] = None,
                      var duration: Option[Long] = None,
                      var status: Status = Statuses.RunningState,
                      var result: String = Result.OK,
                      var resultMessage: Option[String] = None,
                      var partition: Option[String] = None,
                      var reprocessed: Option[ReprocessType] = Some(ReprocessTypes.None),
                      var rowsOK: Option[Long] = None,
                      var rowsKO: Option[Long] = None,
                      var outputPath: Option[String] = None,
                      var archivePath: Option[String] = None,
                      var errorPath: Option[String] = None,
                      var pendingSilver: Option[Boolean] = None,
                      var bronzeDatasetLogId: Option[Int] = None) extends MetaLog