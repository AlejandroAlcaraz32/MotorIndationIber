package com.minsait.indation.metalog.exceptions

class DatabaseException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
