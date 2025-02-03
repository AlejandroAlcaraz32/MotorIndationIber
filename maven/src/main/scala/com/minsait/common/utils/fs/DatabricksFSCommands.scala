package com.minsait.common.utils.fs

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.minsait.common.utils.FileNameUtils

import java.text.SimpleDateFormat
import java.util.concurrent.TimeoutException
import java.util.{Calendar, TimeZone}

object DatabricksFSCommands extends FSCommands {

  override def ls(path: String): Array[String] = {
    dbutils.fs.ls(path).map(_.path).toArray
  }

  override def cp(sourcePath: String, destinationPath: String): Unit ={
    dbutils.fs.cp(sourcePath, destinationPath, recurse = true)
  }

  override def mv(sourcePath: String, destinationPath: String): Unit ={
    dbutils.fs.mv(sourcePath, destinationPath)
  }

  override def rm(path: String): Unit ={
    dbutils.fs.rm(path,recurse = true)
  }

  override def mkdir(path: String): Unit ={
    dbutils.fs.mkdirs(path)
  }

  override def renameFileIfExist(file: String): Unit = {
    if (this.exists(file)) {
      val timeZone = TimeZone.getTimeZone("UTC")
      val calendar = Calendar.getInstance(timeZone)
      val simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm")
      simpleDateFormat.setTimeZone(timeZone)
      val currentTimestamp = simpleDateFormat.format(calendar.getTime)
      val destinationPath = FileNameUtils.addTimestampToFileName(file, currentTimestamp)
      this.logger.info("Renaming file " + file + " to " + destinationPath)
      this.mv(file, destinationPath)
    }
  }

  override def moveSparkWrittenFile(source: String, fileName: String): Unit = {
    var auxDestination = source.substring(0, source.lastIndexOf("/"))
    val finalDestination = auxDestination + "/" + fileName
    auxDestination  = auxDestination.substring(0, auxDestination.lastIndexOf("/")) + "/" +fileName
    val parentFolder = source.substring(0, source.lastIndexOf("/")) + "/" + fileName
    val fileExtension = fileName.split("\\.").last

    val fileToMove = dbutils.fs.ls(source).filter(file => file.name.matches("^(.+)" + fileExtension + "$")).last
    dbutils.fs.mv(fileToMove.path, auxDestination, recurse = false)
    dbutils.fs.rm(parentFolder, recurse = true)
    dbutils.fs.mv(auxDestination, finalDestination, recurse = false)
  }

  override def directoryTree(basePath: String): List[String] = {
    val dirs = dbutils.fs.ls(basePath).filter(_.isDir).map(_.path).toList
    dirs ++ dirs.flatMap(directoryTree)
  }

  override def directoriesWithSources(basePath: String): List[String] = {
    this.directoryTree(basePath).filter(
      dbutils.fs.ls(_).filter(!_.isDir).exists(_.name.matches("^"+sourceString+"(.*)json$")))
  }

  override def directoriesWithDatasets(basePath: String): List[String] = {
    this.directoryTree(basePath).filter(
      dbutils.fs.ls(_).filter(!_.isDir).exists(_.name.matches("^"+datasetString+"(.*)json$")))
  }

  override def directoriesWithSQLConnections(basePath: String): List[String] = {
    this.directoryTree(basePath).filter(
      dbutils.fs.ls(_).filter(!_.isDir).exists(_.name.matches("^"+sqlconnectionString+"(.*)json$")))
  }

  override def directoriesWithTransformations(basePath: String): List[String] = {
    this.directoryTree(basePath).filter(
      dbutils.fs.ls(_).filter(!_.isDir).exists(_.name.matches("^" + transformationsString + "(.*)json$")))
  }

  override def directoriesWithActivities(basePath: String): List[String] = {
    this.directoryTree(basePath).filter(
      dbutils.fs.ls(_).filter(!_.isDir).exists(_.name.matches("^"+activityString+"(.*)json$")))
  }

  override def waitForFinishedCopy(path: String): Unit ={
    var fileSize = dbutils.fs.ls(path).map(_.size).head
    val calendarInstance = Calendar.getInstance()
    calendarInstance.add(Calendar.MINUTE, +30)
    val limitTime = calendarInstance.getTime

    while(fileSize != dbutils.fs.ls(path).map(_.size).head && Calendar.getInstance().getTime.before(limitTime)){
      fileSize = dbutils.fs.ls(path).map(_.size).head
      Thread.sleep(300000)
    }

    if(Calendar.getInstance().getTime.after(limitTime)) throw new TimeoutException("Exceeded limit time while copy a file")
  }

  override def exists(path: String): Boolean = {
    try {
      dbutils.fs.ls(path)
      true
    }
    catch {
      case _ : Throwable => false
    }
  }

  override def fileContent(path: String): String={
    if (exists(path)) {
      val size = dbutils.fs.ls(path)(0).size.toInt
      dbutils.fs.head(path, size)
    }
    else{
      throw new java.io.FileNotFoundException(path)
    }
  }

  override def isDirectory(path: String): Boolean = {
    val listFiles = this.ls(path)

    if (listFiles.length == 1 && listFiles(0).equals(path)) {
      false
    } else {
      true
    }
  }
}
