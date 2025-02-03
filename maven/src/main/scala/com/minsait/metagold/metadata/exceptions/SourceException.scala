package com.minsait.metagold.metadata.exceptions

class SourceException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
