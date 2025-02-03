package com.minsait.indation.metalog.models.enums

object ReprocessTypes extends Enumeration{
  type ReprocessType = Value

  val None : ReprocessTypes.Value = Value("NONE")
  val Partial : ReprocessTypes.Value = Value("PARTIAL")
  val Full : ReprocessTypes.Value = Value("FULL")
}
