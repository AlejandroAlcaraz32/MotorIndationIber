package com.minsait.indation.datalake

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.configuration.models.DatalakeOutputTypes.{Delta, Parquet}
import com.minsait.common.configuration.models.{DatalakeProperties, IndationProperties, LandingProperties}
import com.minsait.common.logging.Logging
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.fs.{FSCommands, FSCommandsFactory}
import com.minsait.indation.datalake.exceptions.{CreateDatabaseException, NewAndModifyException, ReadDatalakeException}
import com.minsait.indation.datalake.readers.LandingCsvFileReader
import com.minsait.indation.metadata.models.enums.IngestionTypes
import com.minsait.indation.metadata.models.{CsvFormat, Dataset, Source, XlsFormat}
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.metalog.models.enums.ReprocessTypes.ReprocessType
import com.minsait.indation.silver.helper.PartitionHelper
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}

import java.io.{File, FileNotFoundException}
import java.nio.file.{Files, NoSuchFileException, Paths, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.util.{Failure, Success}

class DatalakeManager extends Logging with SparkSessionWrapper {

  /* Current timestamp to be included in the output path. */
  private var currentTimestamp = ""

  /* DatalakeProperties object to connect to the datalake. */
  private var datalakeProperties: DatalakeProperties = _

  private var landingProperties: LandingProperties = _

  /* To check if the execution is done in local or in databricks. */
  //  private var executionEnvironment: EnvironmentType = _

  /*  /* Relative path to the archive folder. */
    private val archivePath = "archive/"

    /* Relative path to the error folder. */
    private val errorPath = "error/"*/

  /*  /* Relative path to the versions folder. */
    private val versionsPath = "versions/"*/

  //  /* Relative path to the delta folder. */
  //  private val deltaPath = "delta/"

  /*
    /* Relative path to the parquet or delta forlder. */
    private val dataPath = "data/"
  */

  /* Suffix to add to the historical data folders. */
  private val historicalSuffix = "_historical"

  /*
    private val QAerrorPath = "qa/error/"
  */

  private var fscommands: FSCommands = _

  private var format: String = "delta"

  val tempSuffix = "_temp"


  /* Constructor with the base path. */
  def this(configManager: ConfigurationManager) {
    this()
    //    this.executionEnvironment = configManager.getEnvironment
    this.datalakeProperties = configManager.getDatalakeProperties
    this.landingProperties = configManager.getLandingProperties
    val timeZone = TimeZone.getTimeZone("UTC")
    val calendar = Calendar.getInstance(timeZone)
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm")
    simpleDateFormat.setTimeZone(timeZone)
    this.currentTimestamp = simpleDateFormat.format(calendar.getTime)
    this.fscommands = FSCommandsFactory.getFSCommands(configManager.getEnvironment)
    if (this.datalakeProperties.outputType.isDefined)
      this.format = this.datalakeProperties.outputType.get.value
  }

  def this(indationProperties: IndationProperties) {
    this()
    this.datalakeProperties = indationProperties.datalake
    this.landingProperties = indationProperties.landing
    val timeZone = TimeZone.getTimeZone("UTC")
    val calendar = Calendar.getInstance(timeZone)
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm")
    simpleDateFormat.setTimeZone(timeZone)
    this.currentTimestamp = simpleDateFormat.format(calendar.getTime)
    this.fscommands = FSCommandsFactory.getFSCommands(indationProperties.environment)
    if (this.datalakeProperties.outputType.isDefined)
      this.format = this.datalakeProperties.outputType.get.value
  }

  /* Constructor to inject attributes. Used for testing purposes */
  def this(configManager: ConfigurationManager, currentTimestamp: String) {
    this()
    //    this.executionEnvironment = configManager.getEnvironment
    this.datalakeProperties = configManager.getDatalakeProperties
    this.landingProperties = configManager.getLandingProperties
    this.currentTimestamp = currentTimestamp
    this.fscommands = FSCommandsFactory.getFSCommands(configManager.getEnvironment)
    if (this.datalakeProperties.outputType.isDefined)
      this.format = this.datalakeProperties.outputType.get.value
  }

  def getPendingFiles(source: Source): Array[String] = {
    try {
      val pathToRead = landingProperties.basePath + landingProperties.pendingDirectory + "/" + source.name
      logger.info("Get pending files from: " + pathToRead)

      this.fscommands.ls(pathToRead)
      
    }
    catch {
      case _: NullPointerException => Array[String]()
    }
  }

  def readPendingActualSchema(file: String, csvFormat: CsvFormat): StructType = {
    try {
      // Read the actual schema. This shouldn't be too expensive as Spark's laziness would avoid actually reading the entire file.
      val dfReader = spark.read

      if (csvFormat.escapeChar.isDefined) {
        dfReader.option("escape", csvFormat.escapeChar.get)
      }

      // Using US-ASCII charset, Spark will not be able to read special characters
      // (extended ASCII characters), whereas as UTF-8 they will be read properly.
      var charset = csvFormat.charset
      if (charset.contains("US-ASCII")) {
        charset = "UTF-8"
      }

      dfReader
        .option("header", value = true)
        .option("sep", csvFormat.delimiter)
        .option("charset", charset)
        .option("quote", csvFormat.quote.getOrElse("\""))
        .csv(file)
        .schema
    }
    catch {
      case _: AnalysisException => StructType(Seq())
    }
  }

  def readPendingActualSchemaXls(file: String, xlsxFormat: XlsFormat): StructType = {
    try {
      // Read the actual schema. This shouldn't be too expensive as Spark's laziness would avoid actually reading the entire file.
      val dfReader = spark.read

      if (xlsxFormat.sheet.isDefined && xlsxFormat.dataRange.isDefined) {
        dfReader.option("dataAddress", "'" + xlsxFormat.sheet.get + "'!" + xlsxFormat.dataRange.get)
      }
      else if(xlsxFormat.sheet.isDefined){
        dfReader.option("dataAddress", "'" + xlsxFormat.sheet.get + "'!" + "A1")
      }
      else if (xlsxFormat.dataRange.isDefined){
        dfReader.option("dataAddress", xlsxFormat.dataRange.get)
      }

      dfReader
        .format("excel")
        .option("header", value = true)
        .load(file)
        .schema
    }
    catch {
      case _: AnalysisException => StructType(Seq())
    }
  }

  def readPending(file: String, dataset: Dataset): DataFrame = {
    LandingCsvFileReader.readFile(file, dataset) match {
      case Success(df) => df
      case Failure(_) => spark.emptyDataFrame
    }
  }

  def copyFileToError(file: String, dataBase: String, table: String, fileName: String): String = {

    logger.info("Copy file to error: " + file)

    try {
      val targetPathStr = datalakeProperties.basePath + datalakeProperties.bronzePath + "/" + landingProperties.landingPath + "/" + landingProperties.errorPath + "/" +
        dataBase + "/" + table + "/" + fileName

      this.renameFileIfExists(targetPathStr)

      //      if (executionEnvironment == Local) {
      //        val targetFile = new File(targetPathStr)
      //        targetFile.getParentFile.mkdirs()
      //        Files.copy(Paths.get(file), Paths.get(targetPathStr), StandardCopyOption.REPLACE_EXISTING)
      //      }
      //      else {
      //        dbutils.fs.cp(file, targetPathStr, recurse = true)
      //      }
      fscommands.cp(file, targetPathStr)

      targetPathStr
    }
    catch {
      case ex: NoSuchFileException => throw new ReadDatalakeException("File " + file + " does not exists.", ex.getCause)
      case ex: FileNotFoundException => throw new ReadDatalakeException("File " + file + " does not exists.", ex.getCause)
    }
  }


  def moveFileToLandingArchive(file: String, dataBase: String, table: String, fileName: String): String = {

    logger.info("Move file to landing archive: " + file)

    try {
      val targetPathStr = datalakeProperties.basePath + datalakeProperties.bronzePath + "/" + landingProperties.landingPath + "/" + landingProperties.archivePath + "/" +
        dataBase + "/" + table + "/" + fileName

      this.renameFileIfExists(targetPathStr)

      //      if (executionEnvironment == EnvironmentTypes.Local) {
      //        val targetFile = new File(targetPathStr)
      //        targetFile.getParentFile.mkdirs()
      //        Files.move(Paths.get(file), Paths.get(targetPathStr))
      //      }
      //      else {
      //        dbutils.fs.mv(file, targetPathStr, recurse = true)
      //      }
      fscommands.mv(file, targetPathStr)

      targetPathStr
    }
    catch {
      case ex: NoSuchFileException => throw new ReadDatalakeException("File " + file + " does not exists.", ex.getCause)
    }
  }


  def writeLandingError(df: DataFrame, dataBase: String, table: String, csvFormat: CsvFormat, fileName: String
                        , fileDate: String): String = {
    val pathToSave = datalakeProperties.basePath + datalakeProperties.bronzePath + "/" + landingProperties.landingPath + "/" + landingProperties.errorPath + "/" +
      dataBase + "/" + table + "/" + fileDate + "_" + currentTimestamp + "/" + fileName

    logger.info("Write file to error: " + pathToSave)

    val dfWriter = df.coalesce(1).write.mode(SaveMode.Append)

    if (csvFormat.escapeChar.isDefined) {
      dfWriter.option("escape", csvFormat.escapeChar.get)
    }

    // Using US-ASCII charset, Spark will not be able to read special characters
    // (extended ASCII characters), whereas as UTF-8 they will be read properly.
    var charset = csvFormat.charset
    if (charset.contains("US-ASCII")) {
      charset = "UTF-8"
    }

    if (fileName.endsWith(".gz")) {
      dfWriter.option("compression", "gzip")
    }

    dfWriter
      .option("header", value = false)
      .option("sep", csvFormat.delimiter)
      .option("charset", charset)
      .option("emptyValue", "")
      .option("quote", csvFormat.quote.getOrElse("\""))
      .csv(pathToSave)

    val finalPath = this.moveSparkStructureLandingError(pathToSave, fileName)

    finalPath
  }


  def writeBronze(df: DataFrame, dataBase: String, table: String, fileName: String, fileDate: String): String = {

    var fileNameNew = fileName
    if (fileName.contains(".")) {
      fileNameNew = fileName.split("\\.")(0)
    }

    val baseDatasetPath = datalakeProperties.basePath + datalakeProperties.bronzePath + "/" + datalakeProperties.dataPath + "/" + dataBase + "/" + table + "/" + fileDate + "/"
    val pathToSave = baseDatasetPath + fileNameNew

    logger.info("Write file to bronze: " + pathToSave)

    //    if (executionEnvironment == EnvironmentTypes.Local) {
    //      if (Files.exists(Paths.get(pathToSave))) {
    //        val pathToCheck = baseDatasetPath + versionsPath
    //        var versionsNumber = 0
    //
    //        if (Files.exists(Paths.get(pathToCheck))) {
    //          versionsNumber = new File(pathToCheck)
    //            .listFiles
    //            .count(file => file.getAbsolutePath.matches("^(.+)" + fileNameNew + "_v[0-9]+$"))
    //        }
    //
    //        val pathToMove = pathToCheck + fileNameNew + "_v" + (versionsNumber + 1)
    //        logger.info("Move previous version from " + pathToSave + " to " + pathToMove)
    //        moveFolder(new File(pathToSave), new File(pathToMove))
    //      }
    //    }
    //    else {
    //      if (pathExists(pathToSave)) {
    //        val pathToCheck = baseDatasetPath + versionsPath
    //        var versionsNumber = 0
    //
    //        if (pathExists(pathToCheck)) {
    //          logger.info("Check versionsNumber " + pathToCheck)
    //          versionsNumber = dbutils.fs.ls(pathToCheck).count(_ => true)
    //        }
    //
    //        val pathToMove = pathToCheck + fileNameNew + "_v" + (versionsNumber + 1)
    //        logger.info("Move previous version from " + pathToSave + " to " + pathToMove)
    //        dbutils.fs.mv(pathToSave, pathToMove, recurse = true)
    //      }
    //    }
    // Flujo con fscommands genérico
    if (fscommands.exists(pathToSave)) {
      val pathToCheck = baseDatasetPath + datalakeProperties.versionsPath + "/"
      var versionsNumber = 0

      if (fscommands.exists(pathToCheck)) {
        logger.info("Check versionsNumber " + pathToCheck)
        versionsNumber = fscommands.ls(pathToCheck).count(_ => true)
      }

      val pathToMove = pathToCheck + fileNameNew + "_v" + (versionsNumber + 1)
      logger.info("Move previous version from " + pathToSave + " to " + pathToMove)
      fscommands.mv(pathToSave, pathToMove)
    }

    df.coalesce(1)
      .write
      .option("charset", "UTF-8")
      .mode(SaveMode.Append)
      //      .format("delta")
      .format(this.format)
      .save(pathToSave)

    pathToSave
  }

  private def moveFolder(source: File, destination: File): Unit = {
    if (source.isDirectory) {
      if (!destination.exists()) {
        destination.mkdirs()
      }

      val files = source.list()
      for (file <- files) {
        val srcFile = new File(source, file)
        val destFile = new File(destination, file)
        moveFolder(srcFile, destFile)
      }
    }
    else {
      Files.move(Paths.get(source.getPath), Paths.get(destination.getPath), StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def moveSparkStructureLandingError(source: String, fileName: String): String = {
    var destination = source.substring(0, source.lastIndexOf("/"))
    destination = destination.substring(0, destination.lastIndexOf("/")) + "/" + fileName
    val parentFolder = source.substring(0, source.lastIndexOf("/"))
    val fileExtension = if (source.contains(".gz")) ".csv.gz" else ".csv"

    this.renameFileIfExists(destination)

    //    if (executionEnvironment == EnvironmentTypes.Local) {
    //      val fileToMove = new File(source)
    //        .listFiles
    //        .filter(file => file.getAbsolutePath.matches("^(.+)" + fileExtension + "$")).last
    //      Files.move(Paths.get(fileToMove.getPath), Paths.get(destination))
    //      new Directory(new File(parentFolder)).deleteRecursively()
    //    }
    //    else {
    //      val fileToMove = dbutils.fs.ls(source).filter(file => file.name.matches("^(.+)" + fileExtension + "$")).last
    //      dbutils.fs.mv(fileToMove.path, destination, recurse = false)
    //      dbutils.fs.rm(parentFolder, recurse = true)
    //    }

    // flujo alternativo genérico con fscommands
    val fileToMove =
      fscommands
        .ls(source)
        .filter(
          file => file.split('/').last.matches("^(.+)" + fileExtension + "$")
        )
        .last
    fscommands.mv(fileToMove, destination)
    fscommands.rm(parentFolder)

    destination
  }

  private def renameFileIfExists(file: String): Unit = {
    val fileExtension = this.getFileExtension(file)
    //    if (executionEnvironment == EnvironmentTypes.Local) {
    //      if(Files.exists(Paths.get(file))) {
    //        logger.info("Renaming file (generating version): " + file)
    //        Files.move(Paths.get(file), Paths.get(file.replace(fileExtension, "") + "_v" + currentTimestamp + fileExtension))
    //      }
    //    }
    //    else {
    //      if(this.pathExists(file)) {
    //        logger.info("Renaming file (generating version): " + file)
    //        dbutils.fs.mv(file, file.replace(fileExtension, "") + "_v" + currentTimestamp + fileExtension)
    //      }
    //    }

    // Flujo alternativo genérico con fscommands
    if (fscommands.exists(file)) {
      logger.info("Renaming file (generating version): " + file)
      fscommands.mv(file, file.replace(fileExtension, "") + "_v" + currentTimestamp + fileExtension)
    }
  }

  private def getFileExtension(file: String): String = {
    if (file.contains(".csv.gz")) {
      ".csv.gz"
    } else if (file.contains(".")) {
      "." + file.split("\\.").last
    } else {
      ""
    }
  }

  //  private def pathExists(path: String): Boolean = {
  ////    try {
  //////      dbutils.fs.ls(path)
  ////      fscommands.ls(path)
  ////      true
  ////    }
  ////    catch {
  ////      case _: java.io.FileNotFoundException => false
  ////    }
  //    fscommands.exists(path)
  //  }

  def readBronze(bronzePath: String): DataFrame = {

    logger.info("Read file from bronze: " + bronzePath)

    try {
      spark.read
        //        .format("delta")
        .format(this.format)
        .load(bronzePath)
    }
    catch {
      case _: AnalysisException => spark.emptyDataFrame
    }
  }


  def writeSilver(ingestionMode: IngestionTypes.IngestionType, dataBase: String, table: String, dataFrame: DataFrame
                  , partitionBy: String, primaryKeys: List[String], createDatabase: Boolean, classification: String
                  , timeStampColumnName: String, reprocessType: ReprocessType, allowPartitionChange: Boolean
                  , datasetName: String = ""): String = {


    val dataTablePath = datalakeProperties.basePath + datalakeProperties.silverPath + "/" + classification + "/" + dataBase + "/"
    if (!createDatabase && (!fscommands.exists(dataTablePath) || !databaseExists(dataBase))) {
      throw new CreateDatabaseException("Create dataBase configuration is set to false, " +
        "but datalake path and/or database do not exists for " + dataBase + " dataBase.")
    }

    if (createDatabase) {

      logger.info("Creating dataBase: " + dataBase)
      spark.sql("CREATE DATABASE IF NOT EXISTS " + dataBase)
    }

    val partitions = if (partitionBy.isEmpty) {
      Array[String]()
    } else {
      partitionBy.split("/")
    }

    val pathToWrite = dataTablePath + table
    logger.info("Write file to silver: " + pathToWrite + " using " + ingestionMode + " ingestion mode.")

    ingestionMode match {
      case IngestionTypes.FullSnapshot => writeSilverFull(dataFrame, dataBase, table, pathToWrite, partitions, reprocessType)
      case IngestionTypes.Incremental => writeSilverOnlyNew(dataFrame, dataBase, table, pathToWrite, partitions, reprocessType)
      case IngestionTypes.LastChanges => writeSilverNewAndModify(dataFrame, dataBase, table, pathToWrite, partitions
        , primaryKeys, timeStampColumnName, reprocessType, allowPartitionChange, datasetName)
      case _ => throw new UnsupportedOperationException("Unsupported IngestionType " + ingestionMode.value)
    }

    // In Parquet mode, review if table reconstruction required by schema evolution

    logger.info(s"Creating table ${dataBase}.${table} from file ${pathToWrite}")
    checkTableRecreation(dataBase, table, pathToWrite)
    checkTableRecreation(dataBase, table + historicalSuffix, pathToWrite + historicalSuffix)
    logger.info(s"Created table ${dataBase}.${table} from file ${pathToWrite}")
    // Return path
    pathToWrite
  }


  //  private def datalakePathExists(path: String): Boolean = {
  //    fscommands.exists(path)
  ////    if (executionEnvironment == EnvironmentTypes.Local) {
  ////      Files.exists(Paths.get(path))
  ////    }
  ////    else
  ////      try {
  ////        fscommands.ls(path)
  ////        true
  ////      }
  ////      catch {
  ////        case _: java.io.FileNotFoundException => false
  ////      }
  //
  //  }

  private def databaseExists(dbName: String): Boolean = {
    spark.catalog.databaseExists(dbName)
  }

  private def writeSilverFull(dataFrame: DataFrame, dataBase: String, table: String, pathToWrite: String
                              , partitions: Array[String], reprocessType: ReprocessType): Unit = {

    // Delta or Parquet table.

    if (reprocessType == ReprocessTypes.Partial) {
      writeDataFrame(dataFrame, Array[String](), "false", "true",
        "APPEND", pathToWrite, isFullReprocess = false)
    }
    else if (reprocessType == ReprocessTypes.Full) {

      writeDataFrame(dataFrame, partitions, "false", "true",
        "OVERWRITE", pathToWrite, isFullReprocess = false)
    }
    else {
      writeDataFrame(dataFrame, partitions, "false", "true",
        "OVERWRITE", pathToWrite, isFullReprocess = false)
    }

    if (datalakeProperties.outputType.get == Delta)
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + " USING DELTA LOCATION '" + pathToWrite + "'")
    else if (datalakeProperties.outputType.get == Parquet) {
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + " USING PARQUET LOCATION '" + pathToWrite + "'")
      // EN ESTE CASO, SE HA ESCRITO SIN PARTICIONAR, POR LO QUE NO HAY QUE RESOLVER LAS PARTICIONES
      spark.sql("REFRESH TABLE " + dataBase + "." + table)
    } else throw new UnsupportedOperationException(s"Output type ${datalakeProperties.outputType.get.value} not supported.")

    /*
    // Table _historical, with historical information.
    val pathToWriteAll = pathToWrite + historicalSuffix
    /*
    if (fscommands.exists(pathToWriteAll) && format == Parquet.value)
      performSchemaEvolution(dataFrame, partitions, pathToWriteAll)

    if (reprocessType == ReprocessTypes.Full) {
      writeDataFrame(dataFrame, partitions, "true", "false", "OVERWRITE", pathToWriteAll, isFullReprocess = true)
    }
    else {
      writeDataFrame(dataFrame, partitions, "true", "false", "APPEND", pathToWriteAll, isFullReprocess = false)
    }
    */
    if (datalakeProperties.outputType.get == Delta)
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + historicalSuffix + " USING DELTA LOCATION '" + pathToWriteAll + "'")
    else if (datalakeProperties.outputType.get == Parquet) {
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + historicalSuffix + " USING PARQUET LOCATION '" + pathToWriteAll + "'")
      if (partitions.length > 0) //TODO: VALIDAR SI HAY QUE AÑADIR CONDICIÓN DE dataFrame.count>0
        repairPartitions(dataBase + "." + table + historicalSuffix)
      spark.sql("REFRESH TABLE " + dataBase + "." + table + historicalSuffix)
    } else throw new UnsupportedOperationException(s"Output type ${datalakeProperties.outputType.get.value} not supported.")
    */
  }

  private def writeSilverOnlyNew(dataFrame: DataFrame, dataBase: String, table: String, pathToWrite: String
                                 , partitions: Array[String], reprocessType: ReprocessType): Unit = {

    if (fscommands.exists(pathToWrite) && format == Parquet.value)
      performSchemaEvolution(dataFrame, partitions, pathToWrite)

    if (reprocessType == ReprocessTypes.Full) {
      writeDataFrame(dataFrame, partitions, "true", "false",
        "OVERWRITE", pathToWrite, isFullReprocess = true)
    }
    else {
      writeDataFrame(dataFrame, partitions, "true", "false",
        "APPEND", pathToWrite, isFullReprocess = false)
    }

    if (datalakeProperties.outputType.get == Delta)
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + " USING DELTA LOCATION '" + pathToWrite + "'")
    else if (datalakeProperties.outputType.get == Parquet) {
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + " USING PARQUET LOCATION '" + pathToWrite + "'")
      if (partitions.length > 0) //TODO: VALIDAR SI HAY QUE COMPROBAR SI EL DATAFRAME TENÍA REGISTROS
        repairPartitions(dataBase + "." + table)
      spark.sql("REFRESH TABLE " + dataBase + "." + table)
    } else throw new UnsupportedOperationException(s"Output type ${datalakeProperties.outputType.get.value} not supported.")

  }

  private def writeSilverNewAndModify(dataFrame: DataFrame, dataBase: String, table: String
                                      , pathToWrite: String, partitions: Array[String], primaryKeysCols: List[String]
                                      , timeStampColumnName: String, reprocessTypes: ReprocessType, allowPartitionChange: Boolean
                                      , datasetName: String): Unit = {

    val pathToWriteAll = pathToWrite + historicalSuffix

    var finalDF = dataFrame

    if (primaryKeysCols.nonEmpty && timeStampColumnName.nonEmpty) {

      val w = Window.partitionBy(primaryKeysCols.map(col): _*)

      finalDF = dataFrame
        .withColumn("silverAux", max(col(timeStampColumnName)).over(w))
        .where(col(timeStampColumnName) === col("silverAux"))
        .drop("silverAux").dropDuplicates(primaryKeysCols)
    }
    else {
      logger.error(s"NEW_AND_MODIFIED Empty primarykey and/or timestamp schema column in dataset $datasetName", None)
      throw new NewAndModifyException("NEW_AND_MODIFIED Empty primarykey and/or timestamp schema column")
    }


    // If table exists (first time, it has to be created from scratch).
    //    if (DeltaTable.isDeltaTable(pathToWrite)) {
    if (fscommands.exists(pathToWrite)) {
      performSchemaEvolution(dataFrame, partitions, pathToWrite)
      mergeTable(finalDF, primaryKeysCols, timeStampColumnName, pathToWrite, partitions, allowPartitionChange)
    }
    else {
      writeDataFrame(finalDF, partitions, "false", "false",
        "ERROR", pathToWrite, isFullReprocess = false)
    }

    if (datalakeProperties.outputType.get == Delta)
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + " USING DELTA LOCATION '" + pathToWrite + "'")
    else if (datalakeProperties.outputType.get == Parquet) {
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + " USING PARQUET LOCATION '" + pathToWrite + "'")
      if (partitions.length > 0) //TODO: VALIDAR SI HACE FALTA COMPROBAR QUE EL DATAFRAME TENGA REGISTROS
        repairPartitions(dataBase + "." + table)
      spark.sql("REFRESH TABLE " + dataBase + "." + table)
    } else throw new UnsupportedOperationException(s"Output type ${datalakeProperties.outputType.get.value} not supported.")

    // Table _historical, with historical information.
    /*
    if (fscommands.exists(pathToWriteAll) && format == Parquet.value)
      performSchemaEvolution(dataFrame, partitions, pathToWriteAll)

    if (reprocessTypes == ReprocessTypes.Full) {
      writeDataFrame(dataFrame, partitions, "true", "false",
        "OVERWRITE", pathToWriteAll, isFullReprocess = true)
    }
    else {
      writeDataFrame(dataFrame, partitions, "true", "false",
        "APPEND", pathToWriteAll, isFullReprocess = false)
    }

    if (datalakeProperties.outputType.get == Delta)
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + historicalSuffix + " USING DELTA LOCATION '" + pathToWriteAll + "'")
    else if (datalakeProperties.outputType.get == Parquet) {
      spark.sql("CREATE TABLE IF NOT EXISTS " + dataBase + "." + table + historicalSuffix + " USING PARQUET LOCATION '" + pathToWriteAll + "'")
      if (partitions.length > 0) //TODO: VALIDAR SI HACE FALTA COMPROBAR QUE EL DATAFRAME TENGA REGISTROS
        repairPartitions(dataBase + "." + table + historicalSuffix)
      spark.sql("REFRESH TABLE " + dataBase + "." + table + historicalSuffix)
    } else throw new UnsupportedOperationException(s"Output type ${datalakeProperties.outputType.get.value} not supported.")
    */
  }

  def writeDataFrame(dataFrame: DataFrame, partitionBy: Array[String], isMergeSchema: String = "false", isOverWriteSchema: String = "false"
                     , writeMode: String, pathToWrite: String, isFullReprocess: Boolean): Unit = {

    val mergeSchema = isMergeSchema == "true" || (format == Parquet.value && isOverWriteSchema == "true")

    if (isFullReprocess) {
      if (!dataFrame.isEmpty) {

        val replaceWhere = PartitionHelper.getReplaceWhereFromPartitions(dataFrame, partitionBy)
        dataFrame.write
          .format(this.format)
          .partitionBy(partitionBy: _*)
          .option("mergeschema", mergeSchema)
          .option("overwriteschema", isOverWriteSchema)
          .option("replaceWhere", replaceWhere)
          .mode(writeMode)
          .save(pathToWrite)
      }
    }
    else {
      dataFrame.write
        .format(this.format)
        .partitionBy(partitionBy: _*)
        .option("mergeschema", isMergeSchema)
        .option("overwriteschema", isOverWriteSchema)
        .mode(writeMode)
        .save(pathToWrite)
    }
  }

  def reWriteDataFrame(dataFrame: DataFrame, partitionBy: Array[String], isMergeSchema: String = "false", isOverWriteSchema: String = "false"
                       , writeMode: String, pathToWrite: String, isFullReprocess: Boolean): Unit = {

    // Write final merged data frame in a new temp folder
    val tempPath = pathToWrite + tempSuffix
    if (fscommands.exists(tempPath)) {
      fscommands.rm(tempPath)
    }
    writeDataFrame(dataFrame, partitionBy, isMergeSchema, isOverWriteSchema, writeMode, tempPath, isFullReprocess)

    // Delete final folder and rename temp folder into final folder
    if (fscommands.exists(pathToWrite)) { //siempre debería existir si hemos llamado al método
      fscommands.rm(pathToWrite)
    }
    fscommands.mv(tempPath, pathToWrite)

    // refresh by path
    spark.catalog.refreshByPath(pathToWrite)
  }
  //  private def performDeltaSchemaEvolution(dataFrame: DataFrame, partitions: Array[String], pathToWrite: String): Unit = {
  //
  //    var deltaDF = DeltaTable.forPath(spark, pathToWrite).toDF
  //    val newCols = dataFrame.columns.diff(deltaDF.columns)
  //    val fields = dataFrame.schema.fields
  //
  //    logger.info("FIELDS" + fields.mkString("Array(", ", ", ")"))
  //
  //    if (newCols.length > 0) {
  //      // Schema evolution --> For each new column, a new empty column is added to the current dataframe.
  //      newCols.foreach(aux => {
  //        val col = fields.find(y => y.name == aux).get
  //        deltaDF = deltaDF.withColumn(col.name, lit(null).cast(col.dataType))
  //      })
  //
  //      // Delta table with the new schema is saved (data will not be modified yet).
  //      writeDataFrame(deltaDF, partitions, "true", "false", "OVERWRITE"
  //        , pathToWrite, isFullReprocess = false)
  //    }
  //  }

  private def performSchemaEvolution(dataFrame: DataFrame, partitions: Array[String], pathToWrite: String): Unit = {

    var dataDF =
      if (this.format == Delta.value) {
        DeltaTable.forPath(spark, pathToWrite).toDF
      }
      else if (this.format == Parquet.value) {
        spark.read.option("spark.sql.parquet.int96RebaseModeInRead", "CORRLEGACYECTED").option("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY").parquet(pathToWrite)
      }
      else throw new UnsupportedOperationException(s"Output format ${this.format} not supported")

    val newCols = dataFrame.columns.diff(dataDF.columns)
    val fields = dataFrame.schema.fields

    logger.info("FIELDS" + fields.mkString("Array(", ", ", ")"))

    if (newCols.length > 0) {
      // Schema evolution --> For each new column, a new empty column is added to the current dataframe.
      newCols.foreach(aux => {
        val col = fields.find(y => y.name == aux).get
        dataDF = dataDF.withColumn(col.name, lit(null).cast(col.dataType))
      })

      // Table with the new schema is saved (data will not be modified yet).
      if (format == Parquet.value) {
        // recreation and move
        reWriteDataFrame(dataDF, partitions, "true", "false", "OVERWRITE"
          , pathToWrite, isFullReprocess = false)
      }
      else {
        writeDataFrame(dataDF, partitions, "true", "false", "OVERWRITE"
          , pathToWrite, isFullReprocess = false)
      }
    }

    spark.catalog.refreshByPath(pathToWrite)
  }

  private def mergeTable(dataFrame: DataFrame, primaryKeysCols: List[String], timeStampColumnName: String, pathToWrite: String, partitions: Array[String], allowPartitionChange: Boolean): Unit = {
    if (datalakeProperties.outputType.get == Delta)
      mergeDeltaTable(dataFrame, primaryKeysCols, timeStampColumnName, pathToWrite, partitions, allowPartitionChange)
    else if (datalakeProperties.outputType.get == Parquet)
      mergeParquetTable(dataFrame, primaryKeysCols, timeStampColumnName, pathToWrite, partitions, allowPartitionChange)
  }


  private def mergeDeltaTable(dataFrame: DataFrame, primaryKeysCols: List[String], timeStampColumnName: String, pathToWrite: String, partitions: Array[String], allowPartitionChange: Boolean): Unit = {
    // Prepare all required data structures to update the Delta Table.
    val olddata = "olddata"
    val newdata = "newdata"

    val cols = dataFrame.columns
    val colsWithouthPrimaryKeys = cols.diff(primaryKeysCols)

    var whereCondition = primaryKeysCols
      .map(c => olddata + "." + c + "=" + newdata + "." + c)
      .mkString(" and ")

    //Si hay particionamiento en tabla original y se ha bloqueado su modificación explícitamente, limitar éstas en la consulta para mejorar rendimiento
    //TODO: Hay que valorar si este comportamiento es correcto.
    if (partitions.length > 0 && !allowPartitionChange) {
      // Establecemos la premisa de que las columnas de partición no deberían cambiar en el tipo LastChanges
      whereCondition =
        partitions.map(c => olddata + "." + c + "=" + newdata + "." + c)
          .mkString(" and ") +
          " and " + whereCondition

    }


    val whenMatchedCondition = olddata + "." + timeStampColumnName + "<=" + newdata + "." + timeStampColumnName

    val updateExprMap = colsWithouthPrimaryKeys.map(c => c -> (newdata + "." + c)).toMap
    val insertExprMap = cols.map(c => c -> (newdata + "." + c)).toMap

    logger.info("whereCondition: " + whereCondition)
    logger.info("whenMatchedCondition: " + whenMatchedCondition)
    //logger.info("updateExprMap: " + updateExprMap.toString)
    //logger.info("insertExprMap: " + insertExprMap.toString)

    val dataFrameNew = dataFrame

    try {
      logger.info("Trying to delta upsert directly...")
      val deltaTable = DeltaTable.forPath(spark, pathToWrite)
      deltaTable
        .as("olddata")
        .merge(dataFrameNew.as("newdata"), whereCondition)
        .whenMatched(whenMatchedCondition)
        .updateExpr(updateExprMap)
        .whenNotMatched
        .insertExpr(insertExprMap)
        .execute()
      logger.info("Merge succeeded directly!")
    } catch {
      case e: Exception =>
        logger.info("Direct merge failed, attempting write-then-merge approach...")
        
        // Writing dataFrameNew to newDataPath
        val newDataPath = pathToWrite + "/NEWDATA"
        logger.info("Writing new data into: " + newDataPath)
        dataFrameNew.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(newDataPath)
        
        // Reading data from newDataPath into newDataDF
        logger.info("Reading new data...")
        val newDataDF = spark.read.format("delta").load(newDataPath)
        
        // Attempting merge again with the new data
        logger.info("Trying to delta upsert with new data...")
        DeltaTable.forPath(spark, pathToWrite)
          .as("olddata")
          .merge(newDataDF.as("newdata"), whereCondition)
          .whenMatched(whenMatchedCondition)
          .updateExpr(updateExprMap)
          .whenNotMatched
          .insertExpr(insertExprMap)
          .execute()
        logger.info("Succeed with new data! " + pathToWrite)
        
        // Removing the temporary newDataPath folder
        logger.info("Removing tmp folder " + newDataPath + "...")
        fscommands.rm(newDataPath)
        logger.info("Succeed! " + newDataPath + " removed.")
    }
  }

  private def mergeParquetTable(dataFrame: DataFrame, primaryKeysCols: List[String], timeStampColumnName: String, pathToWrite: String, partitions: Array[String], allowPartitionChange: Boolean): Unit = {
    //TODO: Implementar método merge parquet table
    //    throw new NotImplementedError("MergeParquetTable not implemented")

    // Prepare all required data structures to update the  Table.
    val cols = dataFrame.columns
    val colsWithouthPrimaryKeys = cols.diff(primaryKeysCols)
    var colsPrimaryKey = primaryKeysCols

    //Si hay particionamiento en tabla original y se ha bloqueado su modificación explícitamente, limitar éstas en la consulta para mejorar rendimiento
    //TODO: Hay que valorar si este comportamiento es correcto.
    if (partitions.length > 0 && !allowPartitionChange) {
      // Establecemos la premisa de que las columnas de partición no deberían cambiar en el tipo LastChanges
      colsPrimaryKey = colsPrimaryKey ++ partitions
    }

    // Original data
    val oldDataFrame = spark.read.parquet(pathToWrite)

    // Selection expressions
    val selectModified =
      colsPrimaryKey.sortBy(c => c).map(c => col(c)) ++
        colsWithouthPrimaryKeys.sortBy(c => c).map(c => col("newdata." + c))

    val selectClean =
      colsPrimaryKey.sortBy(c => c).map(c => col(c)) ++
        colsWithouthPrimaryKeys.sortBy(c => c).map(c => col(c))


    // New rows
    val dataDFNew =
      dataFrame
        .join(
          oldDataFrame,
          colsPrimaryKey,
          "leftanti"
        )

    // Rows modified
    val dataDFModified =
      dataFrame
        .alias("newdata")
        .join(
          oldDataFrame.alias("olddata"),
          colsPrimaryKey,
          "inner"
        )

    // Rows not modified
    val dataDFNotModified =
      oldDataFrame
        .join(
          dataFrame,
          colsPrimaryKey,
          "leftanti"
        )

    // Final Dataframe with previous data frames combination (Merge)
    val mergedDF = dataDFNew.select(selectClean: _*)
      .unionAll(dataDFModified.select(selectModified: _*)) // extraemos las más recientes
      .unionAll(dataDFNotModified.select(selectClean: _*))


    // Write final merged data frame in a new temp folder
    reWriteDataFrame(mergedDF, partitions, "false", "true", "OVERWRITE"
      , pathToWrite, isFullReprocess = false)

  }

  //  def deleteBronzeDelta(path: String): Unit = {
  def deleteBronzeData(path: String): Unit = {

    // .dropRight(1) to delete the trailing "/".
    val pathToStop = datalakeProperties.bronzePath + "/" + datalakeProperties.dataPath

    // table delete
    if (this.format == Delta.value) {
      DeltaTable.forPath(spark, path).delete()
    }
    // TODO: else (desconocemos procedimiento en caso Parquet)

    logger.info("Deleting folder path: " + path)
    //    if (executionEnvironment == EnvironmentTypes.Local) {
    //      // Delete file.
    //      new Directory(new File(path)).deleteRecursively()
    //
    //      // Delete parent empty directories.
    //      deleteEmptyParentDirectoriesLocal(path, pathToStop)
    //    }
    //    else if (executionEnvironment == EnvironmentTypes.Databricks) {
    //      // Delete file.
    //      dbutils.fs.rm(path, recurse = true)
    //
    //      // Delete parent empty directories.
    //      deleteEmptyParentDirectoriesDatabricks(path, pathToStop)
    //    }
    //    else if (executionEnvironment == EnvironmentTypes.Synapse) {
    //      // Delete file.
    //      fscommands.rm(path)
    //
    //      // Delete parent empty directories.
    //      deleteEmptyParentDirectoriesSynapse(path, pathToStop)
    //
    //    }

    // Flujo alternativo con fscommands
    // Delete file.
    fscommands.rm(path)
    // Delete parent empty directories.
    deleteEmptyParentDirectories(path, pathToStop)

  }

  /**
   * Deletes empty parent directories in order to clean and simplify folder structure
   *
   * @param filePath   Child folder whose parents will be deleted if they become empty
   * @param pathToStop Path that must not be deleted
   */
  private def deleteEmptyParentDirectories(filePath: String, pathToStop: String): Unit = {
    var empty = true
    var foldersPath = filePath.substring(0, filePath.lastIndexOf("/"))
    logger.info("Parent folder path: " + foldersPath)
    //    this.deleteVersionsDirectoryDatabricksIfExists(foldersPath)
    this.deleteVersionsDirectoryIfExists(foldersPath)
    while (!foldersPath.endsWith(pathToStop) && empty) {
      if (fscommands.ls(foldersPath).isEmpty) {
        fscommands.rm(foldersPath)
        logger.info("Deleting folder path: " + foldersPath)
        foldersPath = foldersPath.substring(0, foldersPath.lastIndexOf("/"))
        logger.info("Parent next folder path: " + foldersPath)
      }
      else {
        empty = false
      }
    }
  }
  //
  //  /**
  //   * Deletes, in local environment, empty parent directories in order to clean and simplify folder structure
  //   * @param filePath Child folder whose parents will be deleted if they become empty
  //   * @param pathToStop Path that must not be deleted
  //   */
  //  private def deleteEmptyParentDirectoriesLocal(filePath: String, pathToStop: String): Unit = {
  //    var empty = true
  //    var foldersPath = filePath.substring(0, filePath.lastIndexOf("/"))
  //    while (!foldersPath.endsWith(pathToStop) && empty) {
  //      val file = new File(foldersPath)
  //      if (file.isDirectory && file.list().isEmpty) {
  //        val directory = new Directory(file)
  //        directory.deleteRecursively()
  //        foldersPath = foldersPath.substring(0, foldersPath.lastIndexOf("/"))
  //      }
  //      else {
  //        empty = false
  //      }
  //    }
  //  }
  //
  //    /**
  //   * Deletes, in Databricks environment, empty parent directories in order to clean and simplify folder structure
  //   * @param filePath Child folder whose parents will be deleted if they become empty
  //   * @param pathToStop Path that must not be deleted
  //   */
  //  private def deleteEmptyParentDirectoriesDatabricks(filePath: String, pathToStop: String): Unit = {
  //    var empty = true
  //    var foldersPath = filePath.substring(0, filePath.lastIndexOf("/"))
  //    logger.info("Parent folder path: " + foldersPath)
  //    this.deleteVersionsDirectoryDatabricksIfExists(foldersPath)
  //    while (!foldersPath.endsWith(pathToStop) && empty) {
  //      if (dbutils.fs.ls(foldersPath).isEmpty) {
  //        dbutils.fs.rm(foldersPath, recurse = true)
  //        logger.info("Deleting folder path: " + foldersPath)
  //        foldersPath = foldersPath.substring(0, foldersPath.lastIndexOf("/"))
  //        logger.info("Parent next folder path: " + foldersPath)
  //      }
  //      else {
  //        empty = false
  //      }
  //    }
  //  }
  //
  //  /**
  //   * Deletes, in Synapse, empty parent directories in order to clean and simplify folder structure
  //   * @param filePath Child folder whose parents will be deleted if they become empty
  //   * @param pathToStop Path that must not be deleted
  //   */
  //  private def deleteEmptyParentDirectoriesSynapse(filePath: String, pathToStop: String): Unit = {
  //    //TODO: Finalizar método
  //    throw new NotImplementedError("Not implemented methos")
  ////    var empty = true
  ////    var foldersPath = filePath.substring(0, filePath.lastIndexOf("/"))
  ////    logger.info("Parent folder path: " + foldersPath)
  ////    this.deleteVersionsDirectorySynapseIfExists(foldersPath)
  ////    while (!foldersPath.endsWith(pathToStop) && empty) {
  ////      if (dbutils.fs.ls(foldersPath).isEmpty) {
  ////        dbutils.fs.rm(foldersPath, recurse = true)
  ////        logger.info("Deleting folder path: " + foldersPath)
  ////        foldersPath = foldersPath.substring(0, foldersPath.lastIndexOf("/"))
  ////        logger.info("Parent next folder path: " + foldersPath)
  ////      }
  ////      else {
  ////        empty = false
  ////      }
  ////    }
  //  }

  private def deleteVersionsDirectoryIfExists(foldersPath: String): Unit = {
    val versionsPath = foldersPath + "/" + this.datalakeProperties.versionsPath + "/"
    if (fscommands.exists(versionsPath)) {
      logger.info("Deleting parent folder versions path: " + versionsPath)
      fscommands.rm(versionsPath)
    }
    //    try {
    //      fscommands.ls(versionsPath)
    //      logger.info("Deleting parent folder versions path: " + versionsPath)
    //      fscommands.rm(versionsPath)
    //    }
    //    catch {
    //      case _: FileNotFoundException => logger.info("Parent folder versions path: " + versionsPath + " not exists")
    //    }
  }

  //  private def deleteVersionsDirectoryDatabricksIfExists(foldersPath: String): Unit = {
  //    val versionsPath = foldersPath + "/" + this.versionsPath
  //    try {
  //      dbutils.fs.ls(versionsPath)
  //      logger.info("Deleting parent folder versions path: " + versionsPath)
  //      dbutils.fs.rm(versionsPath, recurse = true)
  //    }
  //    catch {
  //      case _: FileNotFoundException => logger.info("Parent folder versions path: " + versionsPath + " not exists")
  //    }
  //  }


  //  private def deleteVersionsDirectorySynapseIfExists(foldersPath: String): Unit = {
  //    //TODO: Finalizar método
  //    throw new NotImplementedError("Not implemented methos")
  ////    val versionsPath = foldersPath + "/" + this.versionsPath
  ////    try {
  ////      logger.info("Deleting parent folder versions path: " + versionsPath)
  ////      dbutils.fs.rm(versionsPath, recurse = true)
  ////
  ////    }
  ////    catch {
  ////      case _: FileNotFoundException => logger.info("Parent folder versions path: " + versionsPath + " not exists")
  ////    }
  //  }

  def writeQAError(df: DataFrame, dataBase: String, table: String, subpath: String, fileDate: String): String = {
    writeQA(df, dataBase, table, subpath, qa = datalakeProperties.QAerrorPath, fileDate)
  }

  private def writeQA(df: DataFrame, dataBase: String, table: String, subpath: String, qa: String, fileDate: String): String = {

    val pathToSave = datalakeProperties.basePath + datalakeProperties.silverPath + "/" + qa + "/" + dataBase + "/" + table + "/" + fileDate + "_" + currentTimestamp + "/" + subpath
    df.coalesce(1)
      .write
      .mode(SaveMode.Append)
      //      .format("delta")
      .format(this.format)
      .save(pathToSave)
    pathToSave
  }

  private def checkTableRecreation(database: String, table: String, pathToWrite: String): Unit = {

    val tableName = database + "." + table

    // In case of Parquet and table existing
    if (format == Parquet.value && spark.catalog.tableExists(tableName)) {

      // Get data from hive table and from path
      val dfParquet = spark.read.parquet(pathToWrite)
      val dfHive = spark.sql(s"select * from $tableName")

      // Check if schemas are different
      val newCols1 = dfParquet.columns.diff(dfHive.columns)
      val newCols2 = dfHive.columns.diff(dfParquet.columns)

      // If a column difference is detected, we must drop hive table and create it again
      if (newCols1.length > 0 || newCols2.length > 0) {

        logger.info(s"Reconstructing table $tableName in Hive catalog for schema evolution")

        if (spark.catalog.isCached(tableName)) {
          spark.catalog.uncacheTable(tableName)
        }

        spark.sql(s"DROP TABLE IF EXISTS $tableName")

        spark.catalog.clearCache() //TODO: Revisar - esta operación es peligrosa, podría afectar a otras operaciones en ejecución

        spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING PARQUET LOCATION '" + pathToWrite + "'")

        repairPartitions(tableName)

        spark.catalog.refreshTable(tableName)
      }
    }
  }

  private def repairPartitions(tableName: String) = {
    try {
      spark.sql("MSCK REPAIR TABLE " + tableName)
    } catch {
      case ex: Throwable => logger.error(ex.getMessage, None)
    }
  }

}
