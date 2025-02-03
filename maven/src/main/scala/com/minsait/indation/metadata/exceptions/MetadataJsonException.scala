package com.minsait.indation.metadata.exceptions

class MetadataJsonException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
