package com.minsait.common.configuration.exceptions

class ConfigurationTypeException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
