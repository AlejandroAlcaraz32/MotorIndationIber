package com.minsait.indation.activity.statistics.models

import com.minsait.indation.metadata.models.enums.DatasetTypes.DatasetType
import com.minsait.indation.metadata.models.enums.{IngestionTypes, ValidationTypes}

case class ActivityDataset(
                            name: String,
                            typ: DatasetType,
                            version: Int,
                            ingestion_mode: IngestionTypes.IngestionType,
                            validation_mode: ValidationTypes.ValidationType,
                            partition_by: String
                          )
