package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.CsvHeaderTypes.CsvHeaderType

case class XlsFormat(
                       header: CsvHeaderType,
                       sheet: Option[String],
                       dataRange: Option[String]
)
