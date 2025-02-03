package com.minsait.indation.datalake.readers

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.common.utils.{APIUtils, FileNameUtils}
import com.minsait.indation.metadata.models.enums.ApiAuthenticationTypes
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types.{StringType, StructField}
import org.json.{JSONArray, JSONObject}

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.Base64
import scala.util.{Failure, Success, Try}

case class HttpData(value: String, timestamp: Timestamp)

class LandingApiReader(val indationProperties: IndationProperties) extends SparkSessionWrapper with Logging {

  private val fs: FSCommands = FSCommandsFactory.getFSCommands(indationProperties.environment)

  def readApi(source: Source, dataset: Dataset, lastValue: Option[String]): (DataFrame, String) = {

    // Create HTTP Server and start streaming
    implicit val sqlContext: SQLContext = spark.sqlContext

    /**
     * Lectura del esquema json.
     */

    val schemaFromJson = SchemaHelper.structTypeSchema(dataset).add(StructField("_corrupt_record", StringType, nullable = true))

    implicit val enc: Encoder[HttpData] = Encoders.product[HttpData]
    val stream = MemoryStream[HttpData]

    val avroFileName = FileNameUtils.avroStreamingFileName(dataset)
    val avroFilePath = indationProperties.landing.basePath + indationProperties.landing.streamingDirectory + "/tmp/" + avroFileName

    val checkpointPath = indationProperties.landing.basePath +
      indationProperties.landing.streamingDirectory +
      "/checkpoint/" +
      dataset.name

    // Transforma el campo "value" a json y nos quedamos con sus valores
    //TODO: La columna _corrupt_record se almacena en el fichero avro, por lo que la columna sigue cuando el fichero se mueve a bronze, schema-mismatch y/o invalid
    //TODO: Decidir que hacer con esta columna. Una opción sería al leer el avro completo, borrar el fichero y volver a escribirlo, quitándole antes la columna
    val df = stream.toDF()
      .select(from_json(col("value").cast("string"), schemaFromJson, Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" ->
        "_corrupt_record")).alias("event"))
      .select("event.*")

    // Ejecución del flujo
    var st = df
      .writeStream
      .trigger(Trigger.Once())
      .format("avro")
      .option("compression", "bzip2")
      .option("checkpointLocation", checkpointPath)
      .option("path", avroFilePath)


    val getAPIData = {
      if(dataset.apiInput.get.pagination.typ.value.equals("offset")) {
        apiOffset(source, dataset, stream, st, lastValue, avroFilePath)
      } else if(dataset.apiInput.get.pagination.typ.value.equals("next_link")) {
        apiNextUrl(source, dataset, stream, st, lastValue, avroFilePath)
      } else {
        //Tipo de paginación "none"
        apiUniqueCall(source, dataset, stream, st, lastValue, avroFilePath)
      }
    }

    if(getAPIData) {
      // Lee los datos y los mapea con el esquema Avro
      Try(
        spark
          .read
          .schema(schemaFromJson)
          .format("avro")
          .load(avroFilePath)
      ) match {
        case Success(df) =>
          // Borra los ficheros de checkpoint
          this.fs.rm(checkpointPath)

          (df, avroFilePath)
        case Failure(_) =>
          this.logger.warn("Error reading avro file generated with API response of dataset " + dataset.name)
          // Borra los ficheros avro y el checkpoint
          this.fs.rm(avroFilePath)
          this.fs.rm(checkpointPath)

          (spark.emptyDataFrame, avroFilePath)
      }
    } else {
      if(this.fs.exists(avroFilePath))
        this.fs.rm(avroFilePath)
      if(this.fs.exists(checkpointPath))
        this.fs.rm(checkpointPath)
      (spark.emptyDataFrame, avroFilePath)
    }

  }

  /**
   * Función para la realización de varias llamadas a una API para obtener toda la información de una consulta,
   * de manera que una llamada obtenga los resultados siguientes a la llamada anterior mediante propiedades de top y skip
   * @param source
   * @param dataset
   * @param stream
   * @param st
   * @param lastValue
   * @param avroFilePath
   */
  def apiOffset(source: Source, dataset: Dataset, stream: MemoryStream[HttpData], st: DataStreamWriter[Row],
                lastValue: Option[String], avroFilePath: String): Boolean = {
    /**
     * Bucle para la consulta de datos de forma paginada
     */
    val headers = getHeaders(source, dataset)
    val top = dataset.apiInput.get.pagination.top.get
    val skip = dataset.apiInput.get.pagination.skip.get
    val topSize = dataset.apiInput.get.pagination.topSize.get
    var skipSize = 0

    val url = source.apiConnection.get.url + "/" + dataset.apiInput.get.endpoint
    val parameters = if(dataset.apiInput.get.parameters.isDefined && dataset.apiInput.get.parameters.get.nonEmpty) {
      "?" + dataset.apiInput.get.parameters.get.replace("{lastValue}", if(lastValue.isDefined){ lastValue.get.replace(" ", "+") } else { "" })
    } else {
      ""
    }

    var array = List[String]("init")
    while (array.nonEmpty) {
      array = List()
      // Se ejecuta la consulta
      val topSkip = if(parameters.isEmpty) {
        "?" + top + "=" + topSize + "&" + skip + "=" + skipSize
      } else {
        "&" + top + "=" + topSize + "&" + skip + "=" + skipSize
      }

      val dir = url + parameters + topSkip
      val response = try{
        APIUtils.get(dir, headers)
      } catch {
        case _: Exception => return false
      }

      // Con el tipo offset se puede obtener un JSONObject o un JSONArray

      // Se transforma el valor en un array de json string. Cada json string es un registro (entidad devuelta por el servicio)
      //Se asume que en caso de llegar los datos dentro de una propiedad de la respuesta, dicha respuesta va a ser siempre un object
      val jsonArray = if(dataset.apiInput.get.dataIn.isDefined && dataset.apiInput.get.dataIn.get.nonEmpty){
        val jsonResponse = new JSONObject(response)
        val dataIn = dataset.apiInput.get.dataIn.get
        jsonResponse.getJSONArray(dataIn)
      }
      //Se asume que en caso de llegar los datos directamente a la altura de la respuesta, va a ser siempre un array
      else{
        new JSONArray(response)
      }
      for (n <- 0 until jsonArray.length()) {
        array = jsonArray.getJSONObject(n).toString :: array
      }
      // Se transforma cada registro en la entidad HttpData que contiene el stream
      val rows = array.map(x => HttpData(x,new Timestamp(System.currentTimeMillis())))

      // Añade los elementos obtenidos de la consulta al stream
      stream.addData(rows)
      val stQuery = st.start()
      Try(stQuery.awaitTermination()) match {
        case Success(_) =>
          this.logger.info(s"Reading API response ${dir} to ${avroFilePath}")
          skipSize += topSize
        case Failure(e) =>
          this.logger.error(s"Error reading API response from $dir", Some(e))
          return false
      }
    }

    true
  }

  /**
   * Función para la realización de varias llamadas a una API para obtener toda la información de una consulta,
   * de manera que una llamada obtenga los resultados siguientes a la llamada anterior mediante una nueva url proporcionada
   * por la llamada anterior
   * @param source
   * @param dataset
   * @param stream
   * @param st
   * @param lastValue
   * @param avroFilePath
   */
  def apiNextUrl(source: Source, dataset: Dataset, stream: MemoryStream[HttpData], st: DataStreamWriter[Row],
                 lastValue: Option[String], avroFilePath: String): Boolean = {
    /**
     * Bucle para la consulta de datos de forma paginada
     */
    val headers = getHeaders(source, dataset)
    val nextUrlField = dataset.apiInput.get.pagination.nextUrl.get
    var nextUrl: Option[String] = Some(source.apiConnection.get.url + "/" + dataset.apiInput.get.endpoint)
    nextUrl = if(dataset.apiInput.get.parameters.isDefined && dataset.apiInput.get.parameters.get.nonEmpty) {
      Some(nextUrl.get + "?" + dataset.apiInput.get.parameters.get.replace("{lastValue}", if(lastValue.isDefined){ lastValue.get.replace(" ", "+") } else { "" }))
    } else {
      nextUrl
    }

    while (nextUrl.isDefined) {
      var array = List[String]()
      // Se ejecuta la consulta
      val response = try{
        APIUtils.get(nextUrl.get, headers)
      } catch {
        case _: Exception => return false
      }
      // Se transforma el valor en un array de json string. Cada json string es un registro (entidad devuelta por el servicio)
      // Con el tipo nextUrl siempre se debería obtener un JSONObject (contiene los datos + la siguiente url)
      val jsonResponse = new JSONObject(response)

      //Se asume que en caso de llegar los datos dentro de una propiedad de la respuesta, dicha respuesta va a ser siempre un object
      //En caso de nextUrl, el jsonResponse es siempre un object
      val jsonArray = if(dataset.apiInput.get.dataIn.isDefined && dataset.apiInput.get.dataIn.get.nonEmpty){
        val dataIn = dataset.apiInput.get.dataIn.get
        jsonResponse.getJSONArray(dataIn)
      }else{
        val jsonArray = new JSONArray()
        jsonArray.put(jsonResponse)
        jsonArray
      }

      for (n <- 0 until jsonArray.length()) {
        array = jsonArray.getJSONObject(n).toString :: array
      }

      // Se transforma cada registro en la entidad HttpData que contiene el stream
      val rows = array.map(x => HttpData(x,new Timestamp(System.currentTimeMillis())))

      // Añade los elementos obtenidos de la consulta al stream
      stream.addData(rows)
      val stQuery = st.start()
      Try(stQuery.awaitTermination()) match {
        case Success(_) =>
          this.logger.info(s"Reading API response ${nextUrl.get} to ${avroFilePath}")

          nextUrl = if(jsonResponse.has(nextUrlField)) {
            Some(jsonResponse.getString(nextUrlField))
          } else {
            None
          }
        case Failure(e) =>
          this.logger.error(s"Error reading API response from ${nextUrl.get}", Some(e))
          return false
      }
    }

    true
  }

  /**
   * Función para la realización de una única llamada a una API para obtener toda la información de una consulta
   * @param source
   * @param dataset
   * @param stream
   * @param st
   * @param lastValue
   * @param avroFilePath
   * @return
   */
  def apiUniqueCall(source: Source, dataset: Dataset, stream: MemoryStream[HttpData], st: DataStreamWriter[Row],
                lastValue: Option[String], avroFilePath: String): Boolean = {
    val headers = getHeaders(source, dataset)

    val url = source.apiConnection.get.url + "/" + dataset.apiInput.get.endpoint
    val parameters = if(dataset.apiInput.get.parameters.isDefined && dataset.apiInput.get.parameters.get.nonEmpty) {
      "?" + dataset.apiInput.get.parameters.get.replace("{lastValue}", if(lastValue.isDefined){ lastValue.get.replace(" ", "+") } else { "" })
    } else {
      ""
    }

    var array = List[String]()
    // Se ejecuta la consulta
    val dir = url + parameters
    val response = try{
      APIUtils.get(dir, headers)
    } catch {
      case _: Exception => return false
    }
    // Se transforma el valor en un array de json string. Cada json string es un registro (entidad devuelta por el servicio)
    //Se asume que en caso de llegar los datos dentro de una propiedad de la respuesta, dicha respuesta va a ser siempre un object
    val jsonArray = if(dataset.apiInput.get.dataIn.isDefined && dataset.apiInput.get.dataIn.get.nonEmpty){
      val jsonResponse = new JSONObject(response)
      val dataIn = dataset.apiInput.get.dataIn.get
      jsonResponse.getJSONArray(dataIn)
    }
    //Se asume que en caso de llegar los datos directamente a la altura de la respuesta, va a ser siempre un array
    else{
      new JSONArray(response)
    }
    for (n <- 0 until jsonArray.length()) {
      array = jsonArray.getJSONObject(n).toString :: array
    }
    // Se transforma cada registro en la entidad HttpData que contiene el stream
    val rows = array.map(x => HttpData(x,new Timestamp(System.currentTimeMillis())))

    // Añade los elementos obtenidos de la consulta al stream
    stream.addData(rows)
    val stQuery = st.start()
    Try(stQuery.awaitTermination()) match {
      case Success(_) =>
        this.logger.info(s"Reading API response ${dir} to ${avroFilePath}")
      case Failure(e) =>
        this.logger.error(s"Error reading API response from $dir", Some(e))
        return false
    }

    true
  }

  /**
   * Function that returns dataset headers plus the Authorization header in case of needing authentication for he API
   * @param source
   * @param dataset
   * @return
   */
  def getHeaders(source: Source, dataset: Dataset): Map[String, String] = {
    val headers = dataset.apiInput.get.headers

    val authorization: Map[String, String] = source.apiConnection.get.authenticationType match {
      case ApiAuthenticationTypes.Basic =>
        val user = source.apiConnection.get.getUser(indationProperties)
        val pass = source.apiConnection.get.getPassword(indationProperties)

        val authorization = Base64.getEncoder.encodeToString(s"$user:$pass".getBytes(StandardCharsets.UTF_8))
        Map("Authorization" -> s"Basic $authorization")
      case _ =>
        Map()
    }

     if(headers.isDefined)
       headers.get.++(authorization)
     else
       authorization
  }
}

