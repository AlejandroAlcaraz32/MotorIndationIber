package com.minsait.metagold.logging

import org.apache.log4j.PropertyConfigurator

trait Logging {
  //PropertyConfigurator.configure(this.getClass.getResourceAsStream("/log4j.properties"))

  val logger = new Logger(this.getClass().getName)
}
