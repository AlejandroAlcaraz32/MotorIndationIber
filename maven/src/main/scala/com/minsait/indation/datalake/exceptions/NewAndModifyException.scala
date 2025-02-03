package com.minsait.indation.datalake.exceptions

class NewAndModifyException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
