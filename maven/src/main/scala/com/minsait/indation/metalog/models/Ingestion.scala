package com.minsait.indation.metalog.models

import com.minsait.indation.metalog.MetaInfo.Layers.Layer
import com.minsait.indation.metalog.MetaInfo.Statuses.Status
import com.minsait.indation.metalog.MetaInfo.{Result, Statuses}

case class Ingestion(var ingestionId: Int = 0,
                     var layer: Option[Layer] = None,
                     var executionStart: Long = 0,
                     var executionEnd: Option[Long] = None,
                     var duration: Option[Long] = None,
                     var status: Status = Statuses.RunningState,
                     var result: String = Result.OK) extends MetaLog
