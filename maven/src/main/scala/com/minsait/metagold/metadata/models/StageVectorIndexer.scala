package com.minsait.metagold.metadata.models

case class StageVectorIndexer(
                           name: String,
                           description: String,
                           historicalTable: StageTable,
                           inputCol: Option[String],
                           outputCol: Option[String],
                           maxCategories: Option[Int] = Some(20)
                         )
