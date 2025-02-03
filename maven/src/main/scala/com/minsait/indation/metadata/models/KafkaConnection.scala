package com.minsait.indation.metadata.models

case class KafkaConnection(
        saslMechanism: String,
        securityProtocol: String,
        bootstrapServersKey: String
)
