package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode

case class SQLTransformationStage(
                                   connection: String, // connection to origin and destination database. Destination and source tables must be in the same connection
                                   destSchema: String, // schema where destination table must be
                                   destTable: String, // destination table
                                   sqlSelectExpr: String, // select query with data to be written
                                   filterExpr: Option[String], // filter expression which can use parameters
                                   writeMode: WriteMode, // save mode (overwrite, truncate, append)
                                   cleanAfter: Boolean, // if true, table will be truncated at the end of the activity
                                   dropAfter: Boolean, // if true, table will be dropped at the end of the activity
                                   deleteAfterWhere: Option[String] // If defined, table will perform a delete statement applying the "where" condition defined
                                 ){
  def getSqlTable:String={
    s"$destSchema.$destTable"
  }
}
