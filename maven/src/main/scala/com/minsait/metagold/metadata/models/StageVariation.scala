package com.minsait.metagold.metadata.models

case class StageVariation (
  name: String,
  description: String,
  percentage: Boolean,
  timeLag: Int,
  timeOrderCol: String,
  inputCol: String,
  outputCol: String
  )
