package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.TableTypes.TableType
import com.minsait.metagold.metadata.models.enums.TableTypes.TableType

case class StageTable(
                       name: String,
                       typ: TableType,
                       database: Option[String],
                       table: String,
                       classification: Option[String],
                       sql: Option[String],
                       joinExpr: Option[JoinExpr]
                     )