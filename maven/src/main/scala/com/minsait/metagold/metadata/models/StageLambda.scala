package com.minsait.metagold.metadata.models

case class StageLambda(
                       name: String,
                       hotDataset: String,
                       hotFilterExpr: Option[String],
                       coldTable: Option[StageTable],
                       coldFilterExpr: Option[String]
                     )