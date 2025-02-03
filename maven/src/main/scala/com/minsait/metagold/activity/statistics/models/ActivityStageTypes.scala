//package com.minsait.metagold.activity.statistics.models
//
//import com.minsait.metagold.metadata.models.enums.ValueToStringTypes
//
//object ActivityStageTypes {
//
//  sealed case class ActivityStageType(value: String) extends ValueToStringTypes
//
//  object DatalakeStage extends ActivityStageType("datalake_stage")
//  object SQLSource extends ActivityStageType("sql-source")
//  object SQLStage extends ActivityStageType("sql_stage")
//
//  val values = Seq(DatalakeStage, SQLSource, SQLStage)
//}
