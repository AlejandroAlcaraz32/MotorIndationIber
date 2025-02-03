package com.minsait.common.utils.fs

import com.minsait.common.utils.FileNameUtils
import com.minsait.indation.Main.logger

import java.io.{ByteArrayOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeoutException
import java.util.{Calendar, TimeZone}
import scala.sys.process._
import org.apache.hadoop.fs.Path


// TODO: Rellenar métodos con lógica de Synapse
object SynapseFSCommands extends FSCommands {

  def runCommand(cmd: String): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }

  def lsr(path: String, recursive: Boolean): Array[String] = {
    val comando =
      if (recursive)
        "hdfs dfs -ls -C -R " + path
      else
        "hdfs dfs -ls -C "  + path


    val resultado = runCommand(comando)

    if (resultado._1 != 0){
      // error en la lectura
      throw new RuntimeException(s"Error getting file list from ${path}.\n${resultado._3}")
    } else{
      //resultado._2.split("\\\\n").flatMap(_.split("\\\\t")).filter(_.nonEmpty).toArray
      resultado._2.split("\n")
    }
  }

  override def ls(path: String): Array[String] = {
    //    dbutils.fs.ls(path).map(_.path).toArray
    lsr(path,true)
  }

  // TODO: Para la función cp de Local y Databricks se incluyó la funcionalidad de copiar carpetas completas, pero
  // en Synapse no se ha podido probar, por lo que no es seguro que funcione la copia de carpetas como está ahora mismo
  override def cp(sourcePath: String, destinationPath: String): Unit ={
    //    dbutils.fs.cp(sourcePath, destinationPath)

    val pathDestino = new Path(destinationPath)
    val baseDestinoPath = pathDestino.getParent
    val baseDestinoPathString = baseDestinoPath.toString
    var comandoExiste = s"hdfs dfs -test -d $baseDestinoPathString"
    val resultadoExiste = runCommand(comandoExiste)

    if (resultadoExiste._1 == 1){
      this.mkdir(baseDestinoPathString)

    }

    val comando = s"hdfs dfs -cp $sourcePath $destinationPath"

    val resultado = runCommand(comando)

    if (resultado._1 != 0){

      throw new RuntimeException(s"Error copying ${sourcePath} into ${destinationPath}.\n${resultado._3}")
    }
  }

  override def mv(sourcePath: String, destinationPath: String): Unit ={
    //    dbutils.fs.mv(sourcePath, destinationPath)
    val comando = s"hdfs dfs -mv $sourcePath $destinationPath"

    val resultado = runCommand(comando)

    if (resultado._1 != 0){
      throw new RuntimeException(s"Error moving ${sourcePath} to ${destinationPath}.\n${resultado._3}")
    }
  }

  override def rm(path: String): Unit ={
    //    dbutils.fs.rm(path,recurse = true)

    val comando = s"hdfs dfs -rm -R $path"

    val resultado = runCommand(comando)

    if (resultado._1 != 0){
      throw new RuntimeException(s"Error removing ${path}.\n${resultado._3}")
    }
  }

  override def mkdir(path: String): Unit ={
    //    dbutils.fs.mkdirs(path)
    val comando = s"hdfs dfs -mkdir -p $path"

    val resultado = runCommand(comando)

    if (resultado._1 != 0){
      throw new RuntimeException(s"Error creating folder ${path}.\n${resultado._3}")
    }
  }

  override def exists(path: String): Boolean = {
    val comando = s"hdfs dfs -test -e $path"

    val resultado = runCommand(comando)

    return resultado._1 == 0
  }

  def isDir(path: String): Boolean = {
    val comando = s"hdfs dfs -test -d $path"

    val resultado = runCommand(comando)

    return resultado._1 == 0
  }

  def isEmpty(path: String): Boolean = {
    val comando = s"hdfs dfs -test -s $path"

    val resultado = runCommand(comando)

    return resultado._1 != 0
  }

  def lastFilePath(path: String):String={
    val pathWithoutSlash = if (path.endsWith("/")) path.substring(0,path.length-1) else path
    val lastSlash = pathWithoutSlash.lastIndexOf("/")
    pathWithoutSlash.substring(lastSlash+1)
  }

  def previousFilePath(path: String):String={
    val pathWithoutSlash = if (path.endsWith("/")) path.substring(0,path.length-1) else path
    val lastSlash = pathWithoutSlash.lastIndexOf("/")
    pathWithoutSlash.substring(0,lastSlash)
  }

  def getFileSize(path: String): Long={

    val comando = "hdfs dfs -stat %b " + path

    val result = runCommand(comando)

    if (result._1 != 0){
      throw new RuntimeException(s"Error getting size from ${path}.\n${result._3}")
    }
    else {
      //BigInt.parseBigInt(result._2.trim)
      result._2.trim.toLong
    }

  }

  override def renameFileIfExist(file: String): Unit = {
    /*
        if (exists(file)) {
          val timeZone = TimeZone.getTimeZone("UTC")
          val calendar = Calendar.getInstance(timeZone)
          val simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm")
          simpleDateFormat.setTimeZone(timeZone)
          val currentTimestamp = simpleDateFormat.format(calendar.getTime)
          val destinationPath = FileNameUtils.addTimestampToFileName(file, currentTimestamp)
          logger.info("Renaming file " + file + " to " + destinationPath)
          mv(file, destinationPath)
        }
    */

    if (exists(file)) {
      val timeZone = TimeZone.getTimeZone("UTC")
      val calendar = Calendar.getInstance(timeZone)
      val simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm")
      simpleDateFormat.setTimeZone(timeZone)
      val currentTimestamp = simpleDateFormat.format(calendar.getTime)
      val destinationPath = FileNameUtils.addTimestampToFileName(file, currentTimestamp)
      logger.info("Renaming file " + file + " to " + destinationPath)
      mv(file, destinationPath)
    }
  }

  override def moveSparkWrittenFile(source: String, fileName: String): Unit = {
    //    var auxDestination = source.substring(0, source.lastIndexOf("/"))
    //    val finalDestination = auxDestination + "/" + fileName
    //    auxDestination  = auxDestination.substring(0, auxDestination.lastIndexOf("/")) + "/" +fileName
    //    val parentFolder = source.substring(0, source.lastIndexOf("/")) + "/" + fileName
    //    val fileExtension = fileName.split("\\.").last
    //
    //    val fileToMove = dbutils.fs.ls(source).filter(file => file.name.matches("^(.+)" + fileExtension + "$")).last
    //    dbutils.fs.mv(fileToMove.path, auxDestination, recurse = false)
    //    dbutils.fs.rm(parentFolder, recurse = true)
    //    dbutils.fs.mv(auxDestination, finalDestination, recurse = false)

    var auxDestination = source.substring(0, source.lastIndexOf("/"))
    val finalDestination = auxDestination + "/" + fileName
    auxDestination  = auxDestination.substring(0, auxDestination.lastIndexOf("/")) + "/" +fileName
    val parentFolder = source.substring(0, source.lastIndexOf("/")) + "/" + fileName
    val fileExtension = fileName.split("\\.").last

    val fileToMove = ls(source).filter(file => lastFilePath(file).matches("^(.+)" + fileExtension + "$")).last
    mv(fileToMove, auxDestination)
    rm(parentFolder)
    mv(auxDestination, finalDestination)
  }

  override def directoryTree(basePath: String): List[String] = {
    //    val dirs = dbutils.fs.ls(basePath).filter(_.isDir).map(_.path).toList
    //    dirs ++ dirs.flatMap(directoryTree)
    val dirs = lsr(basePath,true).toList.filter(isDir(_)).toList
    dirs
  }

  override def directoriesWithSources(basePath: String): List[String] = {
    lsr(basePath,true).toList.filter(
      lastFilePath(_).matches("^"+sourceString+"(.*)json$")
    ).map(p=>previousFilePath(p)).distinct
  }

  override def directoriesWithDatasets(basePath: String): List[String] = {
    lsr(basePath,true).toList.filter(
      lastFilePath(_).matches("^"+datasetString+"(.*)json$")
    ).map(p=>previousFilePath(p)).distinct
  }

  override def directoriesWithSQLConnections(basePath: String): List[String] = {
    lsr(basePath,true).toList.filter(
      lastFilePath(_).matches("^"+sqlconnectionString+"(.*)json$")
    ).map(p=>previousFilePath(p)).distinct
  }

  override def directoriesWithTransformations(basePath: String): List[String] = {
    lsr(basePath,true).toList.filter(
      lastFilePath(_).matches("^"+transformationsString+"(.*)json$")
    ).map(p=>previousFilePath(p)).distinct

  }

  override def directoriesWithActivities(basePath: String): List[String] = {
    lsr(basePath,true).toList.filter(
      lastFilePath(_).matches("^"+activityString+"(.*)json$")
    ).map(p=>previousFilePath(p)).distinct
  }

  override def waitForFinishedCopy(path: String): Unit ={
    //    var fileSize = dbutils.fs.ls(path).map(_.size).head
    //    val calendarInstance = Calendar.getInstance()
    //    calendarInstance.add(Calendar.MINUTE, +30)
    //    val limitTime = calendarInstance.getTime
    //
    //    while(fileSize != dbutils.fs.ls(path).map(_.size).head && Calendar.getInstance().getTime.before(limitTime)){
    //      fileSize = dbutils.fs.ls(path).map(_.size).head
    //      Thread.sleep(300000)
    //    }
    //
    //    if(Calendar.getInstance().getTime.after(limitTime)) throw new TimeoutException("Exceeded limit time while copy a file")

    var fileSize = getFileSize(path)
    val calendarInstance = Calendar.getInstance()
    calendarInstance.add(Calendar.MINUTE, +30)
    val limitTime = calendarInstance.getTime

    Thread.sleep(5000)

    while(fileSize != getFileSize(path) && Calendar.getInstance().getTime.before(limitTime)){
      fileSize = getFileSize(path)
      Thread.sleep(30000)
    }

    if(Calendar.getInstance().getTime.after(limitTime)) throw new TimeoutException("Exceeded limit time while copy a file")

  }

  def readText(path: String): String = {
    val comando = "hdfs dfs -cat -ignoreCrc " + path

    val resultado = runCommand(comando)

    if (resultado._1 != 0){
      // error en la lectura
      throw new RuntimeException(s"Error reading file ${path}.\n${resultado._3}")
    } else{
      resultado._2
    }
  }

  def importExternalFile(externalPath: String, localPath: String): String={
    logger.info("Importing external properties file " + externalPath + " into " + localPath)

    val comando = s"hdfs dfs -get -f $externalPath $localPath"

    val resultado = runCommand(comando)

    if (resultado._1 != 0){
      // error en la lectura
      throw new RuntimeException(s"Error importing file ${externalPath} into $localPath.\n${resultado._3}")
    }
    else localPath
  }

  def importExternalFileIntoTemp(externalPath: String): String={
    val outputPath = "/tmp/" + lastFilePath(externalPath)
    importExternalFile(externalPath, outputPath)
  }

  override def fileContent(path: String): String={
    if (exists(path)) {
      readText(path)
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
