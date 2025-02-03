package com.minsait.common.logging.models

import spray.json.DefaultJsonProtocol

case class GelfMessage (
                         host: String,
                         short_message: String,
                         full_message: String,
                         timestamp: Long,
                         level: Int,
                         facility: String,
                         _Severity: String,
                         _Application: String,
                         _LogClass: String,
                         _Environment: String
                       )

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val GelfMessageFormat = jsonFormat10(GelfMessage)
}