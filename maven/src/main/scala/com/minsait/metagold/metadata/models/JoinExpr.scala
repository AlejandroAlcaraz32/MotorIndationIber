package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.JoinTypes.JoinType
import com.minsait.metagold.metadata.models.enums.JoinTypes.JoinType

case class JoinExpr(
                     typ: JoinType,
                   //TODO: Mejorar las opciones de join
                     //                     leftColumns: List[String],
                     //                     rightColumns: List[String]
                     expr: String
                   )