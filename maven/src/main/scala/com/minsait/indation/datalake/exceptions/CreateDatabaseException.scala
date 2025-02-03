package com.minsait.indation.datalake.exceptions

class CreateDatabaseException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
