package com.minsait.common.utils.fs

import com.minsait.common.utils.FileNameUtils
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeoutException
import java.util.{Calendar, TimeZone}
import scala.reflect.io.Directory

object LocalFSCommands extends FSCommands{

  override def ls (path: String) : Array[String] = {
    new File(path).listFiles.map(_.getAbsolutePath)
  }

  override def cp(sourcePath: String, destinationPath: String): Unit ={
    val source = new File(sourcePath)
    val destination = new File(destinationPath)

    if(source.isDirectory)
      FileUtils.copyDirectory(source,destination)
    else
      FileUtils.copyFile(source,destination)
  }

  override def mv(sourcePath: String, destinationPath: String): Unit ={
    val destinationFileName = new File(destinationPath).getName
    val destinationFolder = destinationPath.replace(destinationFileName,"")

    mkdir(destinationFolder)
    Files.move(Paths.get(sourcePath),Paths.get(destinationPath))
  }

  override def rm(path: String): Unit ={
    if(Files.isDirectory(Paths.get(path))){
      FileUtils.cleanDirectory(new File(path))
    }
    Files.delete(Paths.get(path))
  }

  override def mkdir(path: String): Unit ={
    Files.createDirectories(Paths.get(path))
  }

  override def renameFileIfExist(file: String): Unit ={
    if(this.exists(file)) {
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

  override def moveSparkWrittenFile(source: String, fileName: String): Unit ={
    var auxDestination = source.substring(0, source.lastIndexOf("/"))
    val finalDestination = auxDestination + "/" + fileName
    auxDestination  = auxDestination.substring(0, auxDestination.lastIndexOf("/")) + "/" +fileName
    val parentFolder = source.substring(0, source.lastIndexOf("/")) + "/" + fileName
    val fileExtension = fileName.split("\\.").last

    val fileToMove = new File(source)
      .listFiles
      .filter(file => file.getAbsolutePath.matches("^(.+)" + fileExtension + "$")).last
    this.mv(fileToMove.getPath, auxDestination)
    new Directory(new File(parentFolder)).deleteRecursively()
    this.mv(auxDestination, finalDestination)
  }

  override def directoryTree(basePath: String): List[String] = {
    val dirs = new File(basePath).listFiles.filter(_.isDirectory).map(_.getAbsolutePath + "/").toList
    dirs ++ dirs.flatMap(directoryTree)
  }

  override def directoriesWithSources(basePath: String): List[String] = {
    this.directoryTree(basePath)
      .filter(new File(_)
                    .listFiles
                    .exists(_.getAbsolutePath.replace("\\","/").split("/").last.matches("^"+sourceString+"(.*)json$")))
  }

  override def directoriesWithDatasets(basePath: String): List[String] = {
    this.directoryTree(basePath)
      .filter(new File(_)
                    .listFiles
                    .exists(_.getAbsolutePath.replace("\\","/").split("/").last.matches("^"+datasetString+"(.*)json$")))
  }

  override def directoriesWithSQLConnections(basePath: String): List[String] = {
    this.directoryTree(basePath)
      .filter(new File(_)
        .listFiles
        .exists(_.getAbsolutePath.replace("\\","/").split("/").last.matches("^"+sqlconnectionString+"(.*)json$")))
  }

  override def directoriesWithTransformations(basePath: String): List[String] = {
    this.directoryTree(basePath)
      .filter(new File(_)
        .listFiles
        .exists(_.getAbsolutePath.replace("\\","/").split("/").last.matches("^"+transformationsString+"(.*)json$")))
  }

  override def directoriesWithActivities(basePath: String): List[String] = {
    this.directoryTree(basePath)
      .filter(new File(_)
        .listFiles
        .exists(_.getAbsolutePath.replace("\\","/").split("/").last.matches("^"+activityString+"(.*)json$")))
  }

  override def waitForFinishedCopy(path: String): Unit ={
    var fileSize = new File(path).length()
    val calendarInstance = Calendar.getInstance()
    calendarInstance.add(Calendar.MINUTE, +30)
    val limitTime = calendarInstance.getTime

    while(fileSize != new File(path).length() && Calendar.getInstance().getTime.before(limitTime)){
      fileSize = new File(path).length()
      Thread.sleep(300000)
    }

    if(Calendar.getInstance().getTime.after(limitTime)) throw new TimeoutException("Exceeded limit time while copy a file")
  }

  override def exists(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }

  override def fileContent(path: String): String={
    if (exists(path)) {
      scala.io.Source.fromFile(path).mkString
    }
    else{
      throw new java.io.FileNotFoundException(path)
    }
  }

  override def isDirectory(path: String): Boolean = {
    val file = new File(path)

    file.isDirectory
  }

}
