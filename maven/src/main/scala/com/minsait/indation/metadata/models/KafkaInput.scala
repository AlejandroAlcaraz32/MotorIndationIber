package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.TopicOffsetTypes

case class KafkaInput(
                       topicKey            : String,
                       groupId             : String,
                       startingOffsets     : Option[TopicOffsetTypes.TopicOffsetType] = Some(TopicOffsetTypes.Earliest),
                       jaasConfigurationKey: String,
                       avroSchemaUrl       : Option[String],
)
