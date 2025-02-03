package com.minsait.common.configuration.models

case class GraylogProperties(graylogServerUrl: String,
                             host: String,
                             facility: String,
                             application: String,
                             environment: String,
                             showInfo: Boolean,
                             showWarning: Boolean,
                             showError: Boolean
                            )
