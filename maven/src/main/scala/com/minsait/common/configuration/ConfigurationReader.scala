package com.minsait.common.configuration

import com.minsait.common.configuration.models.IndationProperties

trait ConfigurationReader {
  val indationProperties: IndationProperties
}
