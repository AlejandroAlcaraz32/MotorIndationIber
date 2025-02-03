package com.minsait.indation.ssl

import javax.net.ssl.{HostnameVerifier, SSLSession}

object VerifiesAllHostNames extends HostnameVerifier {
  def verify(s: String, sslSession: SSLSession): Boolean = true
}
