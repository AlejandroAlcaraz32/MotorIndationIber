package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.ColumnTransformationTypes.ColumnTransformationType

case class ColumnTransformation(typ: ColumnTransformationType, pattern: Option[String], udf: Option[String])
