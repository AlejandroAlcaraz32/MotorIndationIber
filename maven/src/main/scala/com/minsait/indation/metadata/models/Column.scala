package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType

case class Column(name: String,
                  typ: ColumnsType,
                  description: Option[String],
                  comment: Option[String],
                  isPrimaryKey: Option[Boolean] = Some(false),
                  isTimestamp: Option[Boolean] = Some(false),
                  isPartitionable: Option[Boolean] = Some(false),
                  transformation: Option[ColumnTransformation],
                  decimalParameters: Option[ColumnDecimalParameters],
                  sensitive: Boolean,
                  parentName: Option[String] = None,
                  arrayInfo: Option[ArrayInfo] = None,
                  alias: Option[String] = None,
                  ignorePersistence: Option[Boolean] = None)
