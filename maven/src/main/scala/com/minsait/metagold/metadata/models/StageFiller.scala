package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.FillerInputTypes.FillerInputTypes

case class StageFiller(
                        name: String,
                        description: String,
                        typ: FillerInputTypes,
                        value: String,
                        cols: Option[Array[String]]
                      )