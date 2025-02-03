package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.TransformationTypes.TransformationType

case class Transformation(
                           name: String,
                           description: String,
                           typ: TransformationType,
                           datalakeTransformation: Option[DatalakeTransformation],
                           sqlTransformation: Option[SQLTransformation],
                         )