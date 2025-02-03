package com.minsait.indation.datalake.writers

import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.logging.Logging
import com.minsait.common.utils.FileNameUtils
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.indation.metadata.models.Dataset
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

trait BronzeFileWriter extends Logging {
  this: ConfigurationReader =>

  private val fs: FSCommands = FSCommandsFactory.getFSCommands(this.indationProperties.environment)

  def writeBronze(fileInputPath: String, df: DataFrame, ds: Dataset, header: Boolean=false): String = {
    val fileName = fileInputPath.split("/").last
    val prefixTargetPath = FileNameUtils.destinationSubdirsPath(fileName,
      ds,
      indationProperties.datalake.basePath + indationProperties.datalake.bronzePath + "/" + ds.sourceName + "/" + ds.name + "/")

    DataframeCompressedFileWriter.write(fileInputPath,
      prefixTargetPath,
      df,
      ds,
      fs,
      header)
  }

  def copyToBronze(fileInputPath: String, ds: Dataset): String = {

    val fileName = fileInputPath.split("/").last
    val prefixTargetPath = FileNameUtils.destinationSubdirsPath(fileName,
      ds,
      indationProperties.datalake.basePath + indationProperties.datalake.bronzePath + "/" + ds.sourceName + "/" + ds.name + "/")

    val destinationPath = prefixTargetPath + fileName

    logger.info(s"Ready to copy file ${fileInputPath} into destiantion ${destinationPath}")

    this.fs.renameFileIfExist(destinationPath)
    this.fs.cp(fileInputPath, destinationPath)

    destinationPath
  }

  def copyCompressedToBronze(fileInputPath: String, ds: Dataset, encryptBronze: Boolean = false): String = {


    val fileName = fileInputPath.split("/").last
    logger.info(s"Copying file compressed to bronze ${fileName}")

    val prefixTargetPath = FileNameUtils.destinationSubdirsPath(fileName,
      ds,
      indationProperties.datalake.basePath + indationProperties.datalake.bronzePath + "/" + ds.sourceName + "/" + ds.name + "/")

    val fileExtension = fileName.split("\\.").last
    if (fileExtension == "txt" || fileExtension == "csv" || fileExtension == "json" || fileExtension == "xls" || fileExtension == "xlsx") {
      Try {
        val tmpFile = indationProperties.tmpDirectory + fileName

        logger.info(s"Compressing text file ${fileName} into temp folder ${tmpFile}")
        this.fs.cp(fileInputPath, tmpFile)

        val compressedTmpFile = this.fs.compressGzip(tmpFile)
        val destinationPath = prefixTargetPath + compressedTmpFile.split("/").last

        logger.info(s"Moving compressed text file ${fileName} into datalake ${destinationPath}")
        //Si se va a encriptar posteriormente en bronze, el nombre del fichero tras encriptar contiene el prefijo "encrypted_",
        //por lo que renombraremos el fichero que ya existe y contenga ese prefijo.
        if (encryptBronze) {

          val destinationEncryptedPath = prefixTargetPath + "encrypted_" + compressedTmpFile.split("/").last
          this.fs.renameFileIfExist(destinationEncryptedPath)
        } else {

          this.fs.renameFileIfExist(destinationPath)
        }

        this.fs.mv(compressedTmpFile, destinationPath)
        this.fs.rm(tmpFile)

        logger.info(s"Done with compression of text file ${fileName}")
        destinationPath
      } match {
        case Success(path) => path
        case Failure(e) =>
          this.logger.info("Error al comprimir: " + fileInputPath + ". Guardando en bronze...")
          this.logger.info(e.getMessage)

          this.copyToBronze(fileInputPath, ds)
      }
    }
    else {
      if (encryptBronze) {
        if (!this.fs.isDirectory(fileInputPath)) {
          //Como se va a encriptar posteriormente en bronze, el nombre del fichero tras encriptar contiene el prefijo "encrypted_",
          //por lo que renombraremos el fichero que ya existe y contenga ese prefijo.
          val destinationEncryptedPath = prefixTargetPath + "encrypted_" + fileName
          this.fs.renameFileIfExist(destinationEncryptedPath)

          this.copyToBronze(fileInputPath, ds)
        } else {
          Try {
            val tmpFile = indationProperties.tmpDirectory + fileName

            this.fs.cp(fileInputPath, tmpFile)
            val compressedTmpFile = this.fs.compressZipFolder(tmpFile)
            val destinationPath = prefixTargetPath + compressedTmpFile.split("/").last

            //Como se va a encriptar posteriormente en bronze, el nombre del fichero tras encriptar contiene el prefijo "encrypted_",
            //por lo que renombraremos el fichero que ya existe y contenga ese prefijo.
            val destinationEncryptedPath = prefixTargetPath + "encrypted_" + compressedTmpFile.split("/").last
            this.fs.renameFileIfExist(destinationEncryptedPath)

            this.fs.mv(compressedTmpFile, destinationPath)
            this.fs.rm(tmpFile)
            destinationPath
          } match {
            case Success(path) => path
            case Failure(e) =>
              this.logger.info("Error al comprimir: " + fileInputPath + ". Guardando en bronze...")
              this.logger.info(e.getMessage)
              this.copyToBronze(fileInputPath, ds)
          }
        }
      } else {
        this.copyToBronze(fileInputPath, ds)
      }
    }
  }
}
