package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.WriteModes
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode

case class DatalakeTransformation(
                                   classification: String,
                                   database: String,
                                   table: String,
                                   partition: Option[String],
                                   mode: WriteMode,
                                   cache: Option[Boolean],
                                   stages: List[DatalakeTransformationStage],
                                   qualityRules: Option[QualityRules]
                                 ){

  def getDlkRelativePath:String= {
    s"$classification/$database/$table"
  }

  def getDlkTableName:String={
    if(mode == WriteModes.ViewMode)
      s"$table"
    else
      s"$database.$table"

  }

}