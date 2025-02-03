package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType

case class ArrayInfo(elementType: ColumnsType,
                     transformation: Option[ColumnTransformation],
                     decimalParameters: Option[ColumnDecimalParameters]
                    )
