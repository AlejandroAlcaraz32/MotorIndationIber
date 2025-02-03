package com.minsait.metagold.metadata.models.enums

trait ValueToStringTypes {
  val value: String
  override def toString: String = value
}
