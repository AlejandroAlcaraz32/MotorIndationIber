package com.minsait.metagold.activity.exceptions

class OutputTypeException (private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)