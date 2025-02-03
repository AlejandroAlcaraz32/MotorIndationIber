package com.minsait.indation.ssl

import javax.net.ssl.SSLContext
import org.apache.hc.client5.http.classic.HttpClient
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder

object SSLContextUtils {
  def configHttpsClient(): CloseableHttpClient = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
    HttpClientBuilder.create()
      // .setSSLHostnameVerifier(VerifiesAllHostNames)
      // .setSSLContext(sslContext)
      .build()
  }
}
