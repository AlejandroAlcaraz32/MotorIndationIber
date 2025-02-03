package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.SourceTypes.SourceType

case class Source(
                    sourceId: Option[Int],
                    name: String,
                    description: String,
                    typ: SourceType,
                    kafkaConnection: Option[KafkaConnection],
                    jdbcConnection: Option[JdbcConnection],
                    datasets: List[Dataset],
                    apiConnection: Option[ApiConnection] = None
                  )
