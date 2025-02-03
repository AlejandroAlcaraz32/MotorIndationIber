package com.minsait.common.logging

import org.apache.log4j.PropertyConfigurator

trait Logging {

  // read log4j configuration
  PropertyConfigurator.configure(this.getClass.getResourceAsStream("/log4j.properties"))

  // logger initialization
  val logger = new Logger(this.getClass().getName)
}
