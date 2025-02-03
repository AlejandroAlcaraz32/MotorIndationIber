package com.minsait.common.utils

import com.minsait.common.logging.Logging
import com.minsait.indation.activity.exceptions.IngestionException
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

import java.net.UnknownHostException
import javax.net.ssl.SSLHandshakeException

/**
 * APIUtils 1.0
 * Una forma sencilla de consumir metodos APIREST
 */
object APIUtils extends Logging{

  /**
   * get(), devuelve el resultado de API GET,
   * parametros:
   * url: la url de la API REST en formato String
   * headers (opcional): headers a añadir a la consulta, como par "key","value"
   */
  def get(url: String, headers: Map[String, String] = Map()): String = {
    val httpClient = HttpClientBuilder.create().build()
    val getRequest: HttpGet = try {
      new HttpGet(url)
    } catch {
      case e: Exception => logger.error(s"Malformed url in API call: $url", Some(e)); throw e
    }

    headers.foreach(p => {
      getRequest.addHeader(p._1, p._2)
    })

    try {
      val response = httpClient.execute(getRequest)
      val status = response.getStatusLine

      status.getStatusCode match {
        case 200 => EntityUtils.toString(response.getEntity)
        case _ => {
          logger.error(s"Error obtaining response for API call $url: $status", None)
          throw new IngestionException(s"Error obtaining response for API call $url: $status")
        }
      }
    }
    catch {
      case e: IngestionException => throw e
      case e: ClientProtocolException => logger.error("Introduced URL is not an URL", Some(e)); throw e
      case e: UnknownHostException => logger.error("Introduced URL is not found", Some(e)); throw e
      case e: SSLHandshakeException => logger.error("Objective API has no valid SSL certificate", Some(e)); throw e
      case e: Exception => logger.error("Non identified API source error", Some(e)); throw e
    }
    finally {
      getRequest.releaseConnection()
    }
  }

  /**
   * post(), devuelve el resultado de API POST,
   * parametros:
   * url: la url de la API REST en formato String
   * headers (opcional): headers a añadir a la consulta, como par "key","value"
   * body (opcional): body a añadir al POST
   */
  def post(url: String, headers: Map[String, String] = Map(), body: String = null): String = {
    val httpClient = HttpClientBuilder.create().build()
    val postRequest: HttpPost = try {
      new HttpPost(url)
    } catch {
      case e: Exception => logger.error(s"Malformed url in API call: $url", Some(e)); throw e
    }

    headers.foreach(p => {
      postRequest.addHeader(p._1, p._2)
    })

    if (body != null) {
      postRequest.setEntity(new StringEntity(body))
    }

    try {
      val response = httpClient.execute(postRequest)
      val status = response.getStatusLine

      status.getStatusCode match {
        case 200 => EntityUtils.toString(response.getEntity)
        case _ => {
          logger.error(s"Error obtaining response for API call $url: $status",None)
          throw new IngestionException(s"Error obtaining response for API call $url: $status")
        }
      }
    }
    catch {
      case e: IngestionException => throw e
      case e: ClientProtocolException => logger.error("Introduced URL is not an URL", Some(e)); throw e
      case e: UnknownHostException => logger.error("Introduced URL is not found", Some(e)); throw e
      case e: SSLHandshakeException => logger.error("Objective API has no valid SSL certificate", Some(e)); throw e
      case e: Exception => logger.error("Non identified API source error", Some(e)); throw e
    }
    finally {
      postRequest.releaseConnection()
    }
  }
}
