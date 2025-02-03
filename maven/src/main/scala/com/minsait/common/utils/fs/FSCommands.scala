package com.minsait.common.utils.fs

import com.minsait.common.logging.Logging

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipEntry, ZipInputStream, ZipOutputStream}

trait FSCommands extends Logging {
  def ls(path: String): Array[String]
  def cp(sourcePath: String, destinationPath: String)
  def mv(sourcePath: String, destinationPath: String)
  def rm(path: String)
  def mkdir(path: String)
  def renameFileIfExist(file: String)
  def moveSparkWrittenFile(source: String, fileName: String)
  def directoryTree(basePath: String): List[String]
  def directoriesWithSources(basePath: String): List[String]
  def directoriesWithDatasets(basePath: String): List[String]
  def directoriesWithSQLConnections(basePath: String): List[String]
  def directoriesWithTransformations(basePath: String): List[String]
  def directoriesWithActivities(basePath: String): List[String]
  def waitForFinishedCopy(path: String)
  def exists(path: String): Boolean
  def fileContent(path: String): String
  def isDirectory(path: String): Boolean

  val sourceString = "source"
  val datasetString = "dataset"
  val sqlconnectionString = "sqlconnection"
  val activityString = "activity"
  val transformationsString = "transform"

  def compressGzip(fileName: String): String = {

    val BufferSize = 1024
    val buf = new Array[Byte](BufferSize)
    this.logger.info("Gzip compression: " + fileName)

    val compressType = ".gz"
    val zippedAbsolutePath = s"$fileName$compressType"

    val originalFile = new File(fileName.replace("dbfs:/", "/dbfs/"))

    val zippedFile = new File(zippedAbsolutePath.replace("dbfs:/", "/dbfs/"))

    val in = new BufferedInputStream(new FileInputStream(originalFile))

    val out = new GZIPOutputStream(new FileOutputStream(zippedFile))

    var n = in.read(buf)

    while (n >= 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }

    out.close()
    in.close()
    out.flush()
    zippedAbsolutePath
  }

  def uncompressGzip(fileName: String): String = {
    val BufferSize = 1024
    val buf = new Array[Byte](BufferSize)

    val compressType = ".gz"
    val unzippedAbsolutePath = fileName.replace(compressType, "")
    val originalFile = new File(fileName.replace("dbfs:/", "/dbfs/"))
    val unzippedFile = new File(unzippedAbsolutePath.replace("dbfs:/", "/dbfs/"))

    val in = new GZIPInputStream(new FileInputStream(originalFile))
    val out = new BufferedOutputStream(new FileOutputStream(unzippedFile))

    var n = in.read(buf)
    while (n >= 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    out.close()
    in.close()
    out.flush()
    unzippedAbsolutePath
  }

  def compressZipFolder(folderPath: String, zippedFileName: Option[String] = None): String = {
    val BufferSize = 1024
    val buf = new Array[Byte](BufferSize)

    val compressType = ".zip"
    val zippedAbsolutePath = s"${zippedFileName.getOrElse(folderPath)}$compressType"
    val originalFolder = folderPath.replace("dbfs:/", "/dbfs/")
    val zippedFolder = new File(zippedAbsolutePath.replace("dbfs:/", "/dbfs/"))
    //TODO: usar ls de cada entorno
//    val folder = new File(folderPath)
//    val fileList = folder.listFiles()
    val fileList = this.ls(originalFolder)

    val zip = new ZipOutputStream(new FileOutputStream(zippedFolder))

    for (file <- fileList) {
//      zip.putNextEntry(new ZipEntry(file.getName))
      zip.putNextEntry(new ZipEntry(new File(file).getName))
      val in = new BufferedInputStream(new FileInputStream(file))

      var n = in.read(buf)
      while (n >= 0) {
        zip.write(buf, 0, n)
        n = in.read(buf)
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
    zippedAbsolutePath
  }

  def uncompressZipFolder(filePath: String): String = {
    val BufferSize = 1024
    val buf = new Array[Byte](BufferSize)

    val compressType = ".zip"
    val unzippedAbsolutePath = filePath.replace(compressType, "").replace("dbfs:/", "/dbfs/")
    val originalFile = new File(filePath.replace("dbfs:/", "/dbfs/"))

    mkdir(unzippedAbsolutePath)

    val zip = new ZipInputStream(new FileInputStream(originalFile))

    var zipEntry = zip.getNextEntry

    while (zipEntry != null) {
      val fileName = zipEntry.getName
      val file = new File(unzippedAbsolutePath + "/" + fileName)

      val out = new BufferedOutputStream(new FileOutputStream(file))

      var n = zip.read(buf)
      while (n >= 0) {
        out.write(buf, 0, n)
        n = zip.read(buf)
      }
      out.close()
      zip.closeEntry()

      zipEntry = zip.getNextEntry
    }

    zip.closeEntry()
    zip.close()
    unzippedAbsolutePath
  }
}
