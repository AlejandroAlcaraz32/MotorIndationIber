package com.minsait.common.utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object DateUtils {

	def unixToDateTime(timestamp: Long): String = {

		val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
		val date = new Date(timestamp)
		sdf.format(date)
	}
}
