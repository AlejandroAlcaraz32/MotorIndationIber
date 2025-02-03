package com.minsait.common.logging

import com.minsait.common.logging.models.Level
import GraylogLogger.SendGraylogMessage
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

import java.io.{PrintWriter, StringWriter}

class Logger(val logClass: String) {

  val logger = LoggerFactory.getLogger(logClass)
  MDC.put("prefix", "##INDATION##")

  def error(error: String, ex: Option[Throwable]) = {

    if (ex.isDefined)
      logger.error(error,ex.get)
    else
      logger.error(error)

    SendGraylogMessage(
      error,
      if (ex.isDefined) {
        val sw = new StringWriter
        sw.write(ex.get.getMessage + "\n")
        ex.get.printStackTrace(new PrintWriter(sw))
        sw.toString
      } else {
        error
      },
      Level.Error,
      "ERROR",
      logClass
    )
  }

  def info(info: String) = {

    logger.info(info)

    SendGraylogMessage(
      info,
      info,
      Level.Informational,
      "INFO",
      logClass
    )

  }

  def warn(warn: String) = {

    logger.warn(warn)

    SendGraylogMessage(
      warn,
      warn,
      Level.Warning,
      "WARNING",
      logClass
    )

  }



}



