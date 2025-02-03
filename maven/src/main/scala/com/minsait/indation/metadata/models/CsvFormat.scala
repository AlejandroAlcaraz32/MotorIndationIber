package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.CsvHeaderTypes.CsvHeaderType

case class CsvFormat(
                      charset: String,
                      delimiter: String,
                      header: CsvHeaderType,
                      escapeChar: Option[String],
                      multiline: Option[Boolean],
                      lineSep: Option[String],
                      nullValue: Option[String],
                      quote: Option[String]=Some("\"")
)
