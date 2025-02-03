package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode

case class GoldTableSource(
                            dlkClassification: Option[String],
                            dlkDatabase: Option[String],
                            dlkTable: Option[String],
                            dlkSQL: Option[String],
                            dlkFilterExpr: Option[String],
                            sqlConnection: String,
                            sqlSchema: String,
                            sqlTable: String,
                            sqlWriteMode: WriteMode,
                            cleanAfter: Boolean,
                            dropAfter: Boolean,
                            deleteAfterWhere: Option[String]
                     ){
  def getSqlTable:String={
    s"$sqlSchema.$sqlTable"
  }
  def getDlkRelativePath:String={
    s"${dlkClassification.get}/${dlkDatabase.get}/${dlkTable.get}"
  }


}