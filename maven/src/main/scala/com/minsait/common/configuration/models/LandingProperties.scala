package com.minsait.common.configuration.models

case class LandingProperties(storageAccountName: String, basePath: String, container: String
                             , pendingDirectory: String, unknownDirectory: String, invalidDirectory: String
                             , corruptedDirectory: String, schemaMismatchDirectory: String, streamingDirectory: String
                             , landingPath: String = "landing", errorPath: String = "error", archivePath: String = "archive",
                             storageAccountAccessKey: Option[String]=None)
