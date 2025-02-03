package com.minsait.indation.datalake

import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.compression.FileFormatCompressionConfigurations
import com.minsait.common.utils.FileNameUtils
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.indation.datalake.readers.LandingFileReader
import com.minsait.indation.datalake.writers.DataframeCompressedFileWriter
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.FileFormatTypes
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

trait LandingFileManager {
  this: ConfigurationReader =>

  private val fs: FSCommands = FSCommandsFactory.getFSCommands(this.indationProperties.environment)

  /**
   * Genera el path absoluto del fichero teniendo en cuenta que si se ha establecido un nombre de contenedor,
   * éste nombre puede venir como inicio de la ruta relativa del fichero, de modo que hay que suprimir ese inicio
   */
  def absoluteFilePath(relativePath: String): String = {
//    if (this.indationProperties.landing.container.isEmpty) {
//      this.indationProperties.landing.basePath + relativePath
//    } else {
//      this.indationProperties.landing.basePath +
//        relativePath.replace(this.indationProperties.landing.container + "/", "")
//    }
    // preparamos base path terminado con "/" y relative path que no empiece por "/"
    val base = if(! this.indationProperties.landing.basePath.endsWith("/")) this.indationProperties.landing.basePath+"/" else this.indationProperties.landing.basePath
    val relative = if (relativePath.startsWith("/")) relativePath.substring(1) else relativePath
    if (this.indationProperties.landing.container.nonEmpty
        && relativePath.startsWith(this.indationProperties.landing.container + "/")){
      // se ha definido nombre del contenedor y el relativePath empieza con el nombre del contenedor
      // Hay que eliminar el bloque del conenedor del path y unirlo al base path (el base path ya lleva implícito el contenedor)
      val len = this.indationProperties.landing.container.length+1
      base + relative.substring(len)
    }
    else{
      // No se tiene que unificar nada, simplemente se unen base path y relative path
      base + relative
    }
  }

  def deleteFile(absolutePath: String): Unit = {

    this.fs.rm(absolutePath)
  }

  def copyToCorrupted(fileInputPath: String, ds: Dataset): String = {
    val fileName = fileInputPath.split("/").last

    val destinationPath = FileNameUtils.landingDestinationPath(fileName,
      indationProperties.landing.basePath,
      indationProperties.landing.corruptedDirectory,
      ds) + fileName

    fs.renameFileIfExist(destinationPath)

    fs.cp(fileInputPath, destinationPath)

    destinationPath
  }

  def copyToUnknown(fileInputPath: String): String = {
    val replaceDirectory = fileInputPath.replace(this.indationProperties.landing.basePath, "").split("/")(0)
    val destinationPath = fileInputPath.replace(replaceDirectory, this.indationProperties.landing.unknownDirectory)
    this.fs.renameFileIfExist(destinationPath)
    this.fs.cp(fileInputPath, destinationPath)
    destinationPath
  }

  def copyToInvalid(fileInputPath: String, ds: Dataset): String = {
    val fileName = fileInputPath.split("/").last

    val destinationPath = FileNameUtils.landingDestinationPath(fileName,
      indationProperties.landing.basePath,
      indationProperties.landing.invalidDirectory,
      ds) + fileName

    fs.renameFileIfExist(destinationPath)

    fs.cp(fileInputPath, destinationPath)

    destinationPath
  }

  def copyToSchemaMismatch(fileInputPath: String, dataset: Dataset): String = {
    val fileName = fileInputPath.split("/").last

    val destinationPath = FileNameUtils.landingDestinationPath(fileName,
      indationProperties.landing.basePath,
      indationProperties.landing.schemaMismatchDirectory,
      dataset) + fileName

    fs.renameFileIfExist(destinationPath)

    fs.cp(fileInputPath, destinationPath)

    destinationPath
  }

  def writeToSchemaMismatch(fileInputPath: String, df: DataFrame, dataset: Dataset): String = {
    val fileName = fileInputPath.split("/").last
    val compressionConfig = FileFormatCompressionConfigurations.fileFormatCompressionConfiguration(FileFormatTypes.Csv)
    val destinationfileName = FileNameUtils.fileNameWithCompression(fileName.split("\\.")(0) + ".csv")

    val destinationPath = FileNameUtils.landingDestinationPath(fileName,
      indationProperties.landing.basePath,
      indationProperties.landing.schemaMismatchDirectory,
      dataset) + destinationfileName

    fs.renameFileIfExist(destinationPath)

    df.coalesce(1).write
      .mode(SaveMode.Append)
      .option("header", value = false)
      .option("sep", ",")
      .option("compression", compressionConfig.codec.codec)
      .option("charset", "UTF-8")
      .csv(destinationPath)

    fs.moveSparkWrittenFile(destinationPath, destinationfileName)

    destinationPath
  }

  def writeToInvalid(fileInputPath: String, df: DataFrame, dataset: Dataset, header: Boolean = false): String = {
    val fileName = fileInputPath.split("/").last
    val destinationPath = FileNameUtils.landingDestinationPath(fileName,
      indationProperties.landing.basePath,
      indationProperties.landing.invalidDirectory,
      dataset)

    DataframeCompressedFileWriter.write(fileInputPath,
      destinationPath,
      df,
      dataset,
      fs,
      header)
  }

  def readFileLandingPending(fileInputPath: String, dataset: Dataset): Try[DataFrame] = {
    LandingFileReader(dataset).readFile(fileInputPath, dataset)
  }

}
