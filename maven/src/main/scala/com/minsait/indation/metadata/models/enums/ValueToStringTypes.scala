package com.minsait.indation.metadata.models.enums

trait ValueToStringTypes {
  val value: String
  override def toString: String = value
}
