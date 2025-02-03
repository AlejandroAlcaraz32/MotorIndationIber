package com.minsait.indation.silver.udf

object SilverUdfCatalog {
  private val maskAsterick = (_: String) => "****************"

  val udfCatalog = List(
    ("mask-asterisk", maskAsterick)
  )
}
