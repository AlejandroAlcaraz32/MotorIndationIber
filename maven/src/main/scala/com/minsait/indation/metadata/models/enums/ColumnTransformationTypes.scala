package com.minsait.indation.metadata.models.enums

object ColumnTransformationTypes {
	sealed case class ColumnTransformationType(value: String) extends ValueToStringTypes
	object Comma extends ColumnTransformationType("comma")
	object CommaEng extends ColumnTransformationType("comma-eng")
	object Date extends ColumnTransformationType("date")
	object Integer extends ColumnTransformationType("integer")
	object Udf extends ColumnTransformationType("udf")
}
