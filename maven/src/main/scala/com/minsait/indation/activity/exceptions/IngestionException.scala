package com.minsait.indation.activity.exceptions

class IngestionException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)