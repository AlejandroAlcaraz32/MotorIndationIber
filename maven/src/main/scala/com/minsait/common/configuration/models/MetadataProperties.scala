package com.minsait.common.configuration.models

case class MetadataProperties(
                               storageAccountName: String,
                               basePathIngestion: String,
                               basePathTransform: String,
                               container: String,
                               storageAccountAccessKey: Option[String]=None,
                               datasource: Option[MetadataDatasourceProperties]=None
                            )
