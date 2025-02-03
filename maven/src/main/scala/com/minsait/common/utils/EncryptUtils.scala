package com.minsait.common.utils

import com.minsait.common.configuration.exceptions.ConfigurationException
import com.minsait.common.configuration.models.{IndationProperties, SecurityEncryptionTypes}
import com.minsait.common.logging.Logging
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.io._
import java.security.{MessageDigest, SecureRandom}
import java.util.Arrays
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, KeyGenerator}
import scala.util.Random

object EncryptUtils extends Logging {
  private var masterKey: String = ""
  private var hashSalt: String = ""
  var salt: String = ""
  var keySalt: String = ""
  var algorithm: String = ""
  var algorithm_transformation: String = ""

  def setProperties(indationProperties: IndationProperties): Unit = {
    if(indationProperties.security.encryption.encryptionType != SecurityEncryptionTypes.None){
      if(indationProperties.security.encryption.encryptionAlgorithm.isDefined && indationProperties.security.encryption.masterKey.isDefined
      && indationProperties.security.encryption.hashSalt.isDefined){
        masterKey = indationProperties.getSecret(indationProperties.security.encryption.masterKey.get)
        algorithm = indationProperties.security.encryption.encryptionAlgorithm.get.algorithm.value
        algorithm_transformation = algorithm + "/" + indationProperties.security.encryption.encryptionAlgorithm.get.mode.value +
          "/" + indationProperties.security.encryption.encryptionAlgorithm.get.padding

        hashSalt = indationProperties.getSecret(indationProperties.security.encryption.hashSalt.get)
        salt = Random.alphanumeric.filter( x => x.isLetter || x.isDigit).take(15).mkString
        keySalt = Random.alphanumeric.filter( x => x.isLetter || x.isDigit).take(15).mkString

      } else {
        logger.error("Error: hashSalt, masterKey and/or encryptionAlgorithm are not defined. In case of encryptionType being different from None, hashSalt, masterKey and encryptionAlgorithm are needed", None)
        throw new ConfigurationException("Error: hashSalt, masterKey and/or encryptionAlgorithm are not defined. In case of encryptionType being different from None, hashSalt, masterKey and encryptionAlgorithm are needed")
      }
    }
  }

  def getMasterKey: String = {
    masterKey
  }

  def getHashSalt: String = {
    hashSalt
  }

   //Genera la key que se usará para encriptar los campos.
  def stringKeyGenerator(size: Int): String = {
    val keyGen: KeyGenerator = KeyGenerator.getInstance(algorithm)
    val secureRandom: SecureRandom = new SecureRandom()
    keyGen.init(size, secureRandom)
    val key: Array[Byte] = keyGen.generateKey().getEncoded
    Base64.encodeBase64String(key)
  }

  //UDF que se usará para encriptar los campos pasándole la key generada anteriormente
  //También se usa para encriptar la key con la masterKey del keyvault
  def encryptUDF(key: String, map: Map[String,String], isKey: Boolean = false): UserDefinedFunction = udf( (value: String) => {
    val encryptSalt = if(isKey) map("keySalt") else map("salt")
    val cipher: Cipher = Cipher.getInstance(map("algorithm_transformation"))
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key, map("algorithm")))
    val iv = cipher.getIV

    val cipherText = cipher.doFinal(value.getBytes("UTF-8"))
    val cipherSalt = cipher.doFinal(encryptSalt.getBytes("UTF-8"))
    val byteArrayOutputStream = new ByteArrayOutputStream()

    byteArrayOutputStream.write(iv)
    byteArrayOutputStream.write(cipherSalt)
    byteArrayOutputStream.write(cipherText)

    Base64.encodeBase64String(byteArrayOutputStream.toByteArray)
  }
  )

  //UDF que se usará para desencriptar los campos. La primera se usa únicamente para desencriptar el campo key con  la masterKey. La segunda desencripta el dato fila a fila.
  def decryptUDF(key: String,map: Map[String,String]): UserDefinedFunction = udf((encryptedValue: String) => {
    decryptString(key, encryptedValue,map)
  })

  def decryptUDF(map: Map[String,String]): UserDefinedFunction = udf ((key: String, encryptedValue: String) => {
    decryptString(key, encryptedValue,map)
  })

  def decryptString(key: String, encryptedValue: String, map: Map[String,String]): String = {
    val content = Base64.decodeBase64(encryptedValue)
    val iv = new Array[Byte](16)
    val saltArray = new Array[Byte](map("salt").getBytes("UTF-8").length)
    val contentArray = new Array[Byte](content.length - iv.length - saltArray.length)
    val byteArrayInputStream = new ByteArrayInputStream(content)

    byteArrayInputStream.read(iv)
    byteArrayInputStream.read(saltArray)
    byteArrayInputStream.read(contentArray)

    val cipher: Cipher = Cipher.getInstance(map("algorithm_transformation"))
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key, map("algorithm")), new IvParameterSpec(iv))
    new String(cipher.doFinal(contentArray), "UTF-8")
  }

  //Lee la key, le aplica una salt y la convierte en SecretKeySpec que se pasará por la función de encriptado/desencriptado.
  private def keyToSpec(key: String, algorithm: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = key.getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-256")
    keyBytes = sha.digest(keyBytes)
    keyBytes = Arrays.copyOf(keyBytes, 32)
    new SecretKeySpec(keyBytes, algorithm)
  }

  //Encriptar ficheros completos para la capa bronze
  def encryptFile(inputPath: String): String = {
    val fileName = inputPath.split("/").last
    val newFileName = "encrypted_" + fileName
    val newPath = inputPath.replace(fileName, newFileName)

    doCrypto(Cipher.ENCRYPT_MODE, inputPath, newPath)
  }

  def decryptFile(inputPath: String): String = {
    val fileName = inputPath.split("/").last
    val newFileName = "decrypted_" + fileName.replace("encrypted_", "")
    val newPath = inputPath.replace(fileName, newFileName)

    doCrypto(Cipher.DECRYPT_MODE, inputPath, newPath)
  }

  private def doCrypto(cypherMode: Int, inputPath: String, outputPath: String): String = {
    val cipher: Cipher = Cipher.getInstance(algorithm_transformation)

    val inputFile = new File(inputPath)
    val outputFile = new File(outputPath)
    val inputStream = new FileInputStream(inputFile)

    val (inputBytes, iv) = if(cypherMode == Cipher.DECRYPT_MODE){
      val iv = new Array[Byte](16)
      inputStream.read(iv)
      cipher.init(cypherMode, keyToSpec(masterKey,algorithm), new IvParameterSpec(iv))

      (new Array[Byte](inputFile.length.asInstanceOf[Int] - iv.length), iv)
    } else {
      cipher.init(cypherMode, keyToSpec(masterKey,algorithm))
      val iv = cipher.getIV

      (new Array[Byte](inputFile.length.asInstanceOf[Int]), iv)
    }

    inputStream.read(inputBytes)
    val outputBytes: Array[Byte] = cipher.doFinal(inputBytes)

    val outputStream = new FileOutputStream(outputFile)

    if(cypherMode == Cipher.ENCRYPT_MODE){
      outputStream.write(iv)
    }

    outputStream.write(outputBytes)

    outputStream.flush()
    outputStream.close()
    inputStream.close()

    outputPath
  }
}
