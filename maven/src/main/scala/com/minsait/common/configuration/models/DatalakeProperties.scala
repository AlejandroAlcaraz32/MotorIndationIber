package com.minsait.common.configuration.models

case class DatalakeProperties(
                               storageAccountName: String,
                               basePath: String,
                               container: String,
                               bronzePath: String = "bronze",
                               silverPath: String = "silver",
                               goldPath: String = "gold",
                               versionsPath: String = "versions",
                               dataPath: String = "data",
                               QAerrorPath: String = "qa/error",
                               outputType: Option[DatalakeOutputTypes.DatalakeOutputType] = Some(DatalakeOutputTypes.Delta),
                               storageAccountAccessKey: Option[String] = None
                             )



