package com.minsait.metagold.logging

import com.minsait.common.configuration.models.GraylogProperties
import com.minsait.metagold.logging.models.{Level,GelfMessage}
import scalaj.http.{Http, HttpOptions}
import com.minsait.metagold.logging.models.MyJsonProtocol._
import spray.json._

object GraylogLogger {

  // read optional graylog server configuration
  var graylogProperties: Option[GraylogProperties] = None

  /**
   * Method to send http post message to graylog server.
   * @param message: Message class to be sent
   */
  def SendGraylogMessage(short_message: String,
                         full_message: String,
                         level: Int,
                         severity: String,
                         logClass: String)={

    if (graylogProperties.isDefined) {
      val validSeverity = (
        (level==Level.Error && graylogProperties.get.showError)
          || (level==Level.Warning && graylogProperties.get.showWarning)
          || (level==Level.Informational && graylogProperties.get.showInfo)
        )
      if(validSeverity) {
        try {
          val properties = graylogProperties.get
          val message = InitMessage(
            short_message,
            full_message,
            level,
            severity,
            logClass
          )

          // generamos el Json para enviar a graylog
          val gelfMessage = message.toJson.toString()

          // http post a la URL
          val result = Http(properties.graylogServerUrl)
            .option(HttpOptions.allowUnsafeSSL)
            .postData(gelfMessage)
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .option(HttpOptions.readTimeout(10000))
            .asString
//result.
          if (result.code != 202)
            throw new RuntimeException(s"Error sending graylog message to ${properties.graylogServerUrl}. Code received: ${result.code}")
        }
        catch {
          case ex: Throwable =>
            Console.out.println("Error sendGraylogMessage: " + ex.getMessage)
        }
      }
    }
  }

  /**
   * Initialization of Gelf message to be sent to the graylog server
   */
  def InitMessage(
                   short_message: String,
                   full_message: String,
                   level: Int,
                   severity: String,
                   logClass: String
                 ):GelfMessage={
    val properties = graylogProperties.get
    GelfMessage(
      properties.host,
      if (short_message!=full_message || short_message.length<=100) {
        short_message
      }else{short_message.substring(0,97)+"..."},
      full_message,
      System.currentTimeMillis()/1000,
      level,
      properties.facility,
      severity,
      properties.application,
      logClass,
      properties.environment
    )

  }
}
