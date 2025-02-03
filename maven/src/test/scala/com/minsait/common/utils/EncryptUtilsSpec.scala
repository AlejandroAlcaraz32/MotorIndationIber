package com.minsait.common.utils

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models.{DatabricksProperties, DatabricksSecretsProperties, DatalakeProperties, EncryptionAlgorithm, EncryptionAlgorithmTypes, EncryptionModeTypes, IndationProperties, LandingProperties, MetadataProperties, SecurityEncryptionProperties, SecurityEncryptionTypes, SecurityIdentityProperties, SecurityProperties}
import com.minsait.common.spark.{SparkSessionFactory, SparkSessionWrapper}
import com.minsait.common.utils.EncryptUtils._
import com.minsait.indation.datalake.writers.SilverDeltaParquetTableWriter.spark
import org.apache.spark.sql.functions.{coalesce, col, lit, to_timestamp}
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}

class EncryptUtilsSpec extends AnyFunSuite with SparkSessionWrapper with BeforeAndAfterAll with DatasetComparer {

  private val DirectorySeparator: String = "/"

  private val testResourcesBasePath: String = new File(this.getClass.getResource(DirectorySeparator).getPath)
    .toString
    .replace("\\", DirectorySeparator) + DirectorySeparator

  private val tmpDirectory = testResourcesBasePath + "tmp/"

  private val indationProperties: IndationProperties = IndationProperties(
    MetadataProperties("", testResourcesBasePath, "", "")
    , Some(DatabricksProperties(DatabricksSecretsProperties("")))
    , LandingProperties("AccountName1", testResourcesBasePath + "landing/", "", "pending", "unknown"
      , "invalid", "corrupted", "schema-mismatch", "streaming")
    , DatalakeProperties("", testResourcesBasePath, "")
    , SecurityProperties(SecurityIdentityProperties("","",""), SecurityEncryptionProperties(SecurityEncryptionTypes.Pseudonymization,
      Some("mLtzjLyHZaUzBN34hYuSwSWuK/Drbbw+EuZeFnIzoeA="), Some(true),
      Some(EncryptionAlgorithm(EncryptionAlgorithmTypes.Aes, EncryptionModeTypes.Ctr, "NoPadding")), Some("GXCQ9QgX4QZ4i7K")))
    , Local,
    tmpDirectory
  )

  private var map: Map[String, String] = Map()

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionFactory.configSparkSession(indationProperties)
    EncryptUtils.setProperties(indationProperties)
    map = Map(
      "algorithm" -> EncryptUtils.algorithm,
      "algorithm_transformation" -> EncryptUtils.algorithm_transformation,
      "keySalt" -> EncryptUtils.keySalt,
      "salt" -> EncryptUtils.salt
    )
    spark.sparkContext.broadcast(map)
  }

  test("EncryptUtilsSpec# encrypt and decrypt UDF") {
    val schema = List(
      ("UserName", StringType, true),
      ("Concurrency", LongType, true),
      ("FirstName", StringType, true),
      ("LastName", StringType, true),
      ("Timestamp", StringType, true),
    )

    val dataframe = spark.createDF(
      List(
        ("Pedro", 6L, "Pedro", "Gonzalez", "2022-01-01 00:00:00"),
        ("Juan", 8L, "Juan", "Clavijo", "2022-01-01 00:00:00")
      ),
      schema
    ).withColumn("Timestamp", to_timestamp(col("Timestamp"),"yyyy-MM-dd HH:mm:ss"))

    val keyColName: String = "key"
    val key: String = stringKeyGenerator(256)
    val masterKey = EncryptUtils.getMasterKey

    val finalDf = dataframe
      .withColumn(keyColName, lit(key))
      .withColumn(s"encrypted_$keyColName", encryptUDF(masterKey,map)(col(keyColName)))
      .withColumn("encrypted_UserName", encryptUDF(key,map)(coalesce(col("UserName"),lit("null"))))
      .withColumn("encrypted_LastName", encryptUDF(key,map)(coalesce(col("LastName"),lit("null"))))
      .withColumn(s"decrypted_$keyColName", decryptUDF(masterKey,map)(col(s"encrypted_$keyColName")) )
      .withColumn("decrypted_UserName", decryptUDF(map)(col(s"decrypted_$keyColName"), col("encrypted_UserName")))
      .withColumn("decrypted_LastName", decryptUDF(map)(col(s"decrypted_$keyColName"), col("encrypted_LastName")))

    val originalDf = dataframe.select("UserName", "LastName")
    val decryptedDf = finalDf.select(col("decrypted_UserName").as("UserName"), col("decrypted_LastName").as("LastName"))

    assertSmallDatasetEquality(originalDf, decryptedDf, orderedComparison = false)
    finalDf.show(truncate=false)
  }

  test("EncryptUtilsSpec# encrypt/decrypt csv encrypt OK") {
    val inputPath = "pending/source1/20200101_worldCities_toEncrypt.csv"
    val inputFile = this.indationProperties.landing.basePath + inputPath

    val expectedEncryptedPath = this.indationProperties.landing.basePath + "pending/source1/encrypted_20200101_worldCities_toEncrypt.csv"
    val expectedDecryptedPath = this.indationProperties.landing.basePath + "pending/source1/decrypted_20200101_worldCities_toEncrypt.csv"

    val encryptedFile = encryptFile(inputFile)
    decryptFile(encryptedFile)

    val originalFileDF = spark.read.option("header", "true").csv(inputFile)
    val decryptedFileDF = spark.read.option("header", "true").csv(expectedDecryptedPath)

    assert(Files.exists(Paths.get(expectedEncryptedPath)))
    assert(Files.exists(Paths.get(expectedDecryptedPath)))
    assertSmallDatasetEquality(originalFileDF, decryptedFileDF, orderedComparison = false)
  }

  test("EncryptUtilsSpec# encrypt/decrypt json encrypt OK") {
    val inputPath = "pending/source-json-5/20200101_worldCities_toEncrypt.json"
    val inputFile = this.indationProperties.landing.basePath + inputPath

    val expectedEncryptedPath = this.indationProperties.landing.basePath + "pending/source-json-5/encrypted_20200101_worldCities_toEncrypt.json"
    val expectedDecryptedPath = this.indationProperties.landing.basePath + "pending/source-json-5/decrypted_20200101_worldCities_toEncrypt.json"

    val encryptedFile = encryptFile(inputFile)
    decryptFile(encryptedFile)

    val originalFileDF = spark.read.json(inputFile)
    val decryptedFileDF = spark.read.json(expectedDecryptedPath)

    assert(Files.exists(Paths.get(expectedEncryptedPath)))
    assert(Files.exists(Paths.get(expectedDecryptedPath)))
    assertSmallDatasetEquality(originalFileDF, decryptedFileDF, orderedComparison = false)
  }

  test("EncryptUtilsSpec# encrypt/decrypt parquet encrypt OK") {
    val inputPath = "pending/source3/20200101_worldCities_toEncrypt.snappy.parquet"
    val inputFile = this.indationProperties.landing.basePath + inputPath

    val expectedEncryptedPath = this.indationProperties.landing.basePath + "pending/source3/encrypted_20200101_worldCities_toEncrypt.snappy.parquet"
    val expectedDecryptedPath = this.indationProperties.landing.basePath + "pending/source3/decrypted_20200101_worldCities_toEncrypt.snappy.parquet"

    val encryptedFile = encryptFile(inputFile)
    decryptFile(encryptedFile)

    val originalFileDF = spark.read.parquet(inputFile)
    val decryptedFileDF = spark.read.parquet(expectedDecryptedPath)

    assert(Files.exists(Paths.get(expectedEncryptedPath)))
    assert(Files.exists(Paths.get(expectedDecryptedPath)))
    assertSmallDatasetEquality(originalFileDF, decryptedFileDF, orderedComparison = false)
  }

  test("EncryptUtilsSpec# encrypt/decrypt avro encrypt OK") {
    val inputPath = "pending/source2/20200101_worldCities_toEncrypt.bz2.avro"
    val inputFile = this.indationProperties.landing.basePath + inputPath

    val expectedEncryptedPath = this.indationProperties.landing.basePath + "pending/source2/encrypted_20200101_worldCities_toEncrypt.bz2.avro"
    val expectedDecryptedPath = this.indationProperties.landing.basePath + "pending/source2/decrypted_20200101_worldCities_toEncrypt.bz2.avro"

    val encryptedFile = encryptFile(inputFile)
    decryptFile(encryptedFile)

    val originalFileDF = spark.read.format("avro").load(inputFile)
    val decryptedFileDF = spark.read.format("avro").load(expectedDecryptedPath)

    assert(Files.exists(Paths.get(expectedEncryptedPath)))
    assert(Files.exists(Paths.get(expectedDecryptedPath)))
    assertSmallDatasetEquality(originalFileDF, decryptedFileDF, orderedComparison = false)
  }

  test("EncryptUtilsSpec# encrypt/decrypt compressed gz encrypt OK") {
    val inputPath = "pending/source1/prueba1_toEncrypt.gz"
    val inputFile = this.indationProperties.landing.basePath + inputPath

    val expectedEncryptedPath = this.indationProperties.landing.basePath + "pending/source1/encrypted_prueba1_toEncrypt.gz"
    val expectedDecryptedPath = this.indationProperties.landing.basePath + "pending/source1/decrypted_prueba1_toEncrypt.gz"

    val encryptedFile = encryptFile(inputFile)
    decryptFile(encryptedFile)

    val originalFileDF = spark.read.option("header", "true").csv(inputFile)
    val decryptedFileDF = spark.read.option("header", "true").csv(expectedDecryptedPath)

    assert(Files.exists(Paths.get(expectedEncryptedPath)))
    assert(Files.exists(Paths.get(expectedDecryptedPath)))
    assertSmallDatasetEquality(originalFileDF, decryptedFileDF, orderedComparison = false)
  }
}
