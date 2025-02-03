package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.FileFormatTypes.FileFormatType

case class FileInput (
        format: FileFormatType,
        filePattern: String,
        csv: Option[CsvFormat],
        json: Option[JsonFormat],
        fixed: Option[String],
        xls: Option[XlsFormat]
)
