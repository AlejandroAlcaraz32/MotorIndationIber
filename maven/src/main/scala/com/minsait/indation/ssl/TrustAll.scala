package com.minsait.indation.ssl

import java.security.cert.X509Certificate
import javax.net.ssl.X509TrustManager

object TrustAll extends X509TrustManager {
  val getAcceptedIssuers: Array[X509Certificate] = null
  def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
  def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
}
