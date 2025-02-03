package com.minsait.common.configuration.models

case class MetadataDatasourceProperties(server: String, port: String, database: String,
                                        username: String, secretScope: Option[String], passwordkey: String,
                                        driver: String, protocol: String, options: Map[String, String])
