package com.minsait.indation.datalake.readers

//import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.FileNameUtils
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.indation.metadata.models.{Dataset, Source}
import com.minsait.indation.silver.helper.SchemaHelper
import com.minsait.indation.ssl.SSLContextUtils
import org.apache.commons.io.input.ReaderInputStream
import org.apache.hc.client5.http.classic.methods.HttpGet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger

import java.io.InputStreamReader
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class LandingTopicReader(val indationProperties: IndationProperties) extends SparkSessionWrapper with Logging {

  private val fs: FSCommands = FSCommandsFactory.getFSCommands(indationProperties.environment)

  def readTopic(source: Source, dataset: Dataset): (DataFrame, String) = {

    val topic = indationProperties.getSecret(dataset.kafkaInput.get.topicKey)

    val topicQueryStream: DataFrame = this.topicQueryStream(source, dataset, topic)

    val checkpointPath = indationProperties.landing.basePath +
      indationProperties.landing.streamingDirectory +
      "/checkpoint/" +
      topic

    val avroFileName = FileNameUtils.avroStreamingFileName(dataset)
    val avroFilePath = indationProperties.landing.basePath + indationProperties.landing.streamingDirectory + "/tmp/" + avroFileName

    Try(topicQueryStream.writeStream
    .trigger(Trigger.Once)
    .format("avro")
    .option("compression","bzip2")
    .option("checkpointLocation", checkpointPath)
    .option("path", avroFilePath)
    .start().awaitTermination()) match {
      case Success(_) => this.logger.info(s"""Reading topic  ${topic} to ${avroFilePath}""")
      case Failure(e) =>
        this.logger.error(s"Error reading topic $topic", Some(e))
        this.fs.rm(avroFilePath)
        return (spark.emptyDataFrame, avroFilePath)
    }

    Try(this.fs.moveSparkWrittenFile(avroFilePath, avroFileName)) match {
      case Success(_) => this.logger.info("Create avro file from topic " + avroFilePath)
      case Failure(_) =>
        this.logger.warn("No pending events on topic " + topic)
        this.fs.rm(avroFilePath)
        return (spark.emptyDataFrame, avroFilePath)
    }

    Try(spark.read.format("avro").load(avroFilePath)) match {
      case Success(df) =>  (df, avroFilePath)
      case Failure(_) =>
        this.logger.warn("No pending events on topic " + topic)
        this.fs.rm(avroFilePath)
        (spark.emptyDataFrame, avroFilePath)
    }
  }

  private def topicQueryStream(source: Source, dataset: Dataset, topic: String) = {

    val secret = indationProperties.getSecret(dataset.kafkaInput.get.jaasConfigurationKey)
    val servers = indationProperties.getSecret(source.kafkaConnection.get.bootstrapServersKey)

    val EH_SASL_NAMESPACE = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString"
    password="$secret";"""

    val input_stream_df = spark.readStream
      .format("kafka")
      .option("kafka.sasl.mechanism", source.kafkaConnection.get.saslMechanism)
      .option("kafka.security.protocol", source.kafkaConnection.get.securityProtocol)
      .option("kafka.sasl.jaas.config", EH_SASL_NAMESPACE)
      .option("kafka.batch.size", 5000)
      .option("kafka.bootstrap.servers", servers)
      .option("kafka.request.timeout.ms", "60000")
      .option("subscribe", topic)
      .option("group.id", dataset.kafkaInput.get.groupId)
      .option("startingOffsets", dataset.kafkaInput.get.startingOffsets.get.toString)
      .load()

      // If avroSchemaUrl is defined, uses it to decode Avro column
      // Else, assumes Avro column is not schema encoded and simply cast to string
      if (dataset.kafkaInput.get.avroSchemaUrl.isDefined){
        val avroSchema = this.avroSchemaFromUrl(dataset.kafkaInput.get.avroSchemaUrl.get)
        input_stream_df
          .select(from_avro(col("value"), avroSchema).alias("event"))
          .select("event.*")
      } else {
        input_stream_df
          .select(from_json(col("value").cast("string"), SchemaHelper.structTypeSchema(dataset)).alias("event"))
          .select("event.*")
      }
  }

  private def avroSchemaFromUrl(url: String): String = {
    def getSchema(url: String) = {
      val httpClient = SSLContextUtils.configHttpsClient()
      val httpGet = new HttpGet(url)
      val httpResponse = httpClient.execute(httpGet)
      val inputStreamReader = new InputStreamReader(httpResponse.getEntity().getContent())
      val inputStream = new ReaderInputStream(inputStreamReader, "UTF-8")
      scala.io.Source.fromInputStream(inputStream).mkString
    }

    val maxAttemps = 5
    @tailrec
    def retry(fn: => String)(attempt: Integer): String = {
      this.logger.info("Reading avro schema retry attempt " + attempt)
      Try(fn) match {
        case Success(schema) =>
          this.logger.info("Reading avro schema OK")
          schema
        case _ if attempt <= maxAttemps =>
          retry(fn)(attempt + 1)
        case Failure(e) =>
          this.logger.error(s"Reading avro schema $url failed ", Some(e))
          throw new RuntimeException(e)
      }
    }

    retry(getSchema(url))(1)
  }
}
