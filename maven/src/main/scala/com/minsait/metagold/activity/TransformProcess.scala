package com.minsait.metagold.activity

import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.DatalakeOutputTypes.Parquet
import com.minsait.common.configuration.models.{DatalakeOutputTypes, IndationProperties}
import com.minsait.common.spark.SparkSessionFactory.persistedDataframes
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.DataHelper._
import com.minsait.common.utils.{DataHelper, DateUtils, GoldConstants}
//import com.minsait.common.utils.FileNameUtils.GoldDirectory
import com.minsait.metagold.activity.statistics.models.ActivityResults.{Fail, Success}
import com.minsait.metagold.activity.statistics.models.{ActivityDuration, ActivityResults, ActivityTransformation, ActivityTransformationStage, GoldValidator}
import com.minsait.metagold.logging.Logging
import com.minsait.metagold.metadata.MetadataFilesManager
import com.minsait.metagold.metadata.models.enums.TransformationTypes.{Datalake, SQL}
import com.minsait.metagold.metadata.models.enums.WriteModes
import com.minsait.metagold.metadata.models.{DatalakeTransformation, SQLTransformation, Transformation}
import org.apache.commons.cli.CommandLine
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.sql.Timestamp
import scala.util.Try

class TransformProcess(override val indationProperties: IndationProperties,
                       val triggerId: String,
                       val parameters: CommandLine,
                       val uuid: String)
  extends SparkSessionWrapper with ConfigurationReader with Logging {

  def execute(transform: Transformation,
              metadataManager: MetadataFilesManager): ActivityTransformation = {

    val startTimeMillis = System.currentTimeMillis
    var executionResult = ActivityTransformation(
      transform.name,
      transform.typ.value,
      Fail,
      ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(startTimeMillis), 0),
      List(),
      "",
      List("")
    )

    try {

      this.logger.info(s"Starting gold transformation: ${transform.name}. UUID: $uuid")

      //en función del tipo se invocará un procesamiento u otro
      transform.typ match {
        case Datalake => {
          executionResult = processDatalake(transform.datalakeTransformation.get, executionResult, transform.name)
        }
        case SQL => {
          executionResult = processSQL(transform.sqlTransformation.get, metadataManager, executionResult, transform.name)
        }
        case other => {
          val msg = s"Transformation type not defined: $other for transformation ${transform.name}. UUID: $uuid"
          logger.error(msg, None)
          val t = new Timestamp(startTimeMillis)
          return executionResult.copy(
            resultMsg = msg,
            transformationStages = List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(t, t, 0)))
          )
        }
      }

      if (executionResult.transformationResult == Success)
        logger.info(s"Transformation ${transform.name} executed Successfully.")
      else
        logger.error(s"Transformation ${transform.name} Failed. ${executionResult.resultMsg}", None)

      //TODO: Revisar necesidad de "unpersist" para datasets cacheados

      // Return result statistics
      executionResult

    }
    catch {
      case e: Throwable =>
        val msg = s"Unexpected error raised in transformation ${transform.name}."
        logger.error(msg, Some(e))
        executionResult.copy(resultMsg = s"$msg\n${e.getMessage}")
    }
  }

  // Procesamiento de transformación de Datalake
  def processDatalake(transformation: DatalakeTransformation,
                      transformationStatistics: ActivityTransformation,
                      transformationName: String): ActivityTransformation = {

    var resultStatistics = transformationStatistics

    try {
      // Crear dataframe de inicio vacío con las columnas de parametros
      var dataframe = addParameterColumns(spark.emptyDataFrame)

      //Procesar en orden cada etapa de la transformación
      transformation.stages.foreach(stage => {
        if (resultStatistics.resultMsg == "") {
          val startTimeMillis = System.currentTimeMillis
          val activityStage = ActivityTransformationStage(stage.name, stage.typ.value, Fail, 0, ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(startTimeMillis), 0))
          try {

            // Process stage
            dataframe =
              DlkStageProcess.execute(
                stage,
                dataframe,
                uuid,
                indationProperties
              )

            // Add parameter columns
            //            dataframe = addParameterColumns(dataframe).cache()
            //            persistedDataframes = persistedDataframes ++ List(dataframe)
            dataframe = addParameterColumns(dataframe)

            // Stage statistics
            val endTimeMillis = System.currentTimeMillis
            resultStatistics = resultStatistics.copy(
              transformationStages = resultStatistics.transformationStages ++ List(
                activityStage.copy(
                  result = Success,
                  //TODO: Evaluar un método mejor para obtener el recuento a posteriori
                  rows = -1, //dataframe.count(),
                  duration = ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
                )
              )
            )
          }
          catch {
            case e: Throwable =>
              val msg = s"Error catched in stage ${stage.name} of transformation $transformationName."
              val endTimeMillis = System.currentTimeMillis
              logger.error(msg, Some(e))
              resultStatistics = resultStatistics.copy(
                transformationStages = resultStatistics.transformationStages ++ List(activityStage),
                transformationDuration = ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis),
                resultMsg = s"$msg\n${e.getMessage}"
              )
          }
        }
      })

      //Una vez tenemos el dataframe final, eliminamos las columnas de parámetros y añadimos las columnas de estadística (UUID y LOAD_DATE)
      logger.info(s"Processing final columns for transformation")
      dataframe = dropParameterColumns(dataframe)
      var QualityMsg = List("")
      if (transformation.qualityRules.isDefined) {
        val result = GoldValidator.validateQuality(transformation.qualityRules.get, dataframe, transformationName)
        QualityMsg = result._3
        persistedDataframes = persistedDataframes ++ List(result._1,result._2)
      }
      dataframe = addStorageInfoColumns(dataframe, uuid)

      // Si se ha solicitado llevar el dataframe a cache, lo hacemo
      if (transformation.cache.isDefined && transformation.cache.get) {
        dataframe.cache()
        persistedDataframes = persistedDataframes ++ List(dataframe)
      }

      // Finalmente se escribe el dataframe en su destino
      logger.info(s"Writing datalake transformation to ${transformation.getDlkRelativePath}")
      writeDlkTable(
        indationProperties,
        dataframe,
        transformation.getDlkRelativePath,
        transformation.getDlkTableName,
        transformation.mode,
        transformation.partition
      )

      if (transformation.mode != WriteModes.ViewMode) {
        if (!spark.catalog.databaseExists(transformation.database)) {
          logger.info(s"Creating dataBase: ${transformation.database}")
          spark.sql(s"CREATE DATABASE IF NOT EXISTS ${transformation.database}")
        }

        val tableExists = Try {
          spark.read.table(s"${transformation.database}.${transformation.table}").take(1)
          true
        }.getOrElse(false)

        if (!tableExists) {
          logger.info(s"" +
            s"Creating table: ${transformation.database}.${transformation.table}")
          spark.sql(
            s"CREATE TABLE IF NOT EXISTS ${transformation.database}.${transformation.table} " +
            s"USING ${getOutputType(indationProperties.datalake.outputType.getOrElse(DatalakeOutputTypes.Delta)).toUpperCase()} " +
            s"LOCATION '${getDlkPath(indationProperties, transformation.getDlkRelativePath, indationProperties.datalake.goldPath)}'")//GoldDirectory)}'")
        }
      }

      if (indationProperties.datalake.outputType.get == Parquet) {
        checkTableRecreation(indationProperties, transformation.database, transformation.table, getDlkPath(indationProperties, transformation.getDlkRelativePath, indationProperties.datalake.goldPath)) //GoldDirectory))
        repairPartitions(indationProperties, s"${transformation.database}.${transformation.table}")
      }


      // llegados a este punto, podemos determinar que el procesamiento ha tenido éxito
      val endTimeMillis = System.currentTimeMillis
      resultStatistics.copy(
        transformationResult = Success,
        transformationDuration = ActivityDuration(resultStatistics.transformationDuration.start, new Timestamp(endTimeMillis), endTimeMillis - resultStatistics.transformationDuration.start.getTime),
        qualityMsg = QualityMsg//TODO: Comprobar esta resta de Timestamp y Milisegundos
      )

    }
    catch {
      case e: Throwable =>
        val msg = s"Unexpected error raised in datalake transformation $transformationName."
        val endTimeMillis = System.currentTimeMillis
        logger.error(msg, Some(e))

        if (resultStatistics.transformationStages.length == 0) {
          // No podemos dejar vacío el array de etapas de transformación porque luego no lo inserta bien en la tabla de estadísticas, por lo que le metemos un dummy
          val t = new Timestamp(System.currentTimeMillis())
          resultStatistics = resultStatistics.copy(
            transformationStages = List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(t, t, 0)))
          )
        }

        resultStatistics.copy(
          transformationDuration = ActivityDuration(resultStatistics.transformationDuration.start, new Timestamp(endTimeMillis), endTimeMillis - resultStatistics.transformationDuration.start.getTime), //TODO: Comprobar esta resta de Timestamp y Milisegundos
          resultMsg = s"$msg\n${e.getMessage}"
        )
    }
  }

  //Procesamiento de transformación de SQL
  def processSQL(transformation: SQLTransformation,
                 metadataManager: MetadataFilesManager,
                 transformationStatistics: ActivityTransformation,
                 transformationName: String): ActivityTransformation = {

    var resultStatistics = transformationStatistics

    try {
      logger.info(s"Processing transformation SQL")

      // Importar todas las tablas de tipo staging
      transformation.sourceTables.foreach(sourceTable => {

        val startTimeMillis = System.currentTimeMillis
        val activityStage = ActivityTransformationStage(sourceTable.sqlTable, "dlkSourceToSQL", Fail, 0, ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(startTimeMillis), 0))

        val connection = metadataManager.sqlConnectionByName(sourceTable.sqlConnection).get

        var dfSource = {
          if (sourceTable.dlkTable.isDefined && sourceTable.dlkClassification.isDefined && sourceTable.dlkDatabase.isDefined) {
            // Read gold table
            logger.info(s"Importing gold table ${sourceTable.getDlkRelativePath} into connection: ${sourceTable.sqlConnection} / table: ${sourceTable.getSqlTable}")
            DataHelper.readGoldTable(
              indationProperties,
              sourceTable.getDlkRelativePath
            )
          }
          else if (sourceTable.dlkSQL.isDefined) {
            // Read spark SQL expression
            logger.info(s"Importing sql expression into connection: ${sourceTable.sqlConnection} / table: ${sourceTable.getSqlTable}")
            spark.sql(sourceTable.dlkSQL.get)
          }
          else {
            val msg = s"Datalake staging source for table ${sourceTable.sqlTable} not properly defined in transformation."
            resultStatistics = resultStatistics.copy(transformationStages = resultStatistics.transformationStages ++ List(activityStage))
            throw new NoSuchElementException(msg)
          }

        }
        dfSource.persist(StorageLevel.MEMORY_AND_DISK_SER)
        // Add parameters to data frame
        logger.info("Dropping if exists datalaque columns...")
        dfSource = dfSource.drop("datalaque_qr_rejected", "datalaque_qr_rejected_rule")
        
        dfSource = addParameterColumns(dfSource)
        

        // Apply filter if defined (adding parameters)
        if (sourceTable.dlkFilterExpr.isDefined)
          dfSource = dfSource.where(sourceTable.dlkFilterExpr.get)

        // Write SQL Table
        logger.info(s"Writting source ${sourceTable.getSqlTable} from connection ${sourceTable.sqlConnection} into ")
        DataHelper.writeSQLTable(
          connection,
          sourceTable.getSqlTable,
          sourceTable.sqlWriteMode,
          dropParameterColumns(dfSource), // dropping parameters
          indationProperties
        )
        logger.info("Write succeed!")
        dfSource.unpersist()
        // Stage OK statistics
        val endTimeMillis = System.currentTimeMillis
        resultStatistics = resultStatistics.copy(
          transformationStages = resultStatistics.transformationStages ++ List(
            activityStage.copy(
              result = Success,
              //TODO: Evaluar otras opciones para recuento
              rows = -1, //dfSource.count(),
              duration = ActivityDuration(activityStage.duration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
            )
          )
        )
      })

      //Procesar todas las etapas
      transformation.stages.foreach(stage => {

        val startTimeMillis = System.currentTimeMillis
        val activityStage = ActivityTransformationStage(stage.destTable, "sqlExpression", Fail, 0, ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(startTimeMillis), 0))

        // Read SQL
        val connection = metadataManager.sqlConnectionByName(stage.connection).get
        logger.info(s"Executing Stage, read SQL Expression ${stage.sqlSelectExpr}")
        var dataFrameSQL = DataHelper.readSQLTable(
          connection,
          None,
          Some(stage.sqlSelectExpr),
          indationProperties
        )

        // add param columns
        dataFrameSQL = addParameterColumns(dataFrameSQL)

        // apply filter, if defined
        if (stage.filterExpr.isDefined) {
          dataFrameSQL = dataFrameSQL.where(expr(stage.filterExpr.get))
        }

        // Write result
        logger.info(s"Writting SQL Stage Expression into ${stage.getSqlTable}")
        DataHelper.writeSQLTable(
          connection,
          stage.getSqlTable,
          stage.writeMode,
          dropParameterColumns(dataFrameSQL),
          indationProperties
        )

        // Stage OK statistics
        val endTimeMillis = System.currentTimeMillis
        resultStatistics = resultStatistics.copy(
          transformationStages = resultStatistics.transformationStages ++ List(
            activityStage.copy(
              result = Success,
              //TODO: Evaluar otras opciones para recuento
              rows = -1, //dataFrameSQL.count(),
              duration = ActivityDuration(activityStage.duration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
            )
          )
        )
      })

      //Limpiar y eliminar tablas marcadas
      transformation.sourceTables.foreach(sourceTable => {
        if (sourceTable.deleteAfterWhere.isDefined && !sourceTable.deleteAfterWhere.get.isEmpty) { //limpieza parcial basada en where
          logger.info(s"Cleaning source ${sourceTable.getSqlTable} partially where: ${sourceTable.deleteAfterWhere.get}")
          val query = s"delete ${sourceTable.getSqlTable} where ${sourceTable.deleteAfterWhere.get}"
          logger.info(query)
          DataHelper.executeNonQuery(
            metadataManager.sqlConnectionByName(sourceTable.sqlConnection).get,
            indationProperties,
            query
          )
        }
        if (sourceTable.cleanAfter) { //limpieza parcial
          logger.info(s"Cleaning source ${sourceTable.getSqlTable} totally")
          DataHelper.truncateTable(
            metadataManager.sqlConnectionByName(sourceTable.sqlConnection).get,
            indationProperties,
            sourceTable.getSqlTable
          )
        }
        if (sourceTable.dropAfter) { //eliminación
          logger.info(s"Dropping source ${sourceTable.getSqlTable}")
          DataHelper.dropTable(
            metadataManager.sqlConnectionByName(sourceTable.sqlConnection).get,
            indationProperties,
            sourceTable.getSqlTable
          )
        }
      })
      transformation.stages.foreach(stage => {
        if (stage.deleteAfterWhere.isDefined && !stage.deleteAfterWhere.get.isEmpty) { //limpieza parcial basada en where
          logger.info(s"Cleaning source ${stage.getSqlTable} partially where: ${stage.deleteAfterWhere.get}")
          val query = s"delete ${stage.getSqlTable} where ${stage.deleteAfterWhere.get}"
          logger.info(query)
          DataHelper.executeNonQuery(
            metadataManager.sqlConnectionByName(stage.connection).get,
            indationProperties,
            query
          )
        }
        if (stage.cleanAfter) { //limpieza total (truncate)
          logger.info(s"Cleaning source ${stage.getSqlTable} totally")
          DataHelper.truncateTable(
            metadataManager.sqlConnectionByName(stage.connection).get,
            indationProperties,
            stage.getSqlTable
          )
        }
        if (stage.dropAfter) { //eliminación
          logger.info(s"Dropping source ${stage.getSqlTable}")
          DataHelper.dropTable(
            metadataManager.sqlConnectionByName(stage.connection).get,
            indationProperties,
            stage.getSqlTable
          )
        }
      })

      // llegados a este punto, podemos determinar que el procesamiento ha tenido éxito
      val endTimeMillis = System.currentTimeMillis
      resultStatistics.copy(
        transformationResult = Success,
        transformationDuration = ActivityDuration(resultStatistics.transformationDuration.start, new Timestamp(endTimeMillis), endTimeMillis - resultStatistics.transformationDuration.start.getTime) //TODO: Validar el cálculo de esta duración
      )
    }
    catch {
      case e: Throwable =>
        val msg = s"Unexpected error raised in SQL transformation $transformationName."
        logger.error(msg, Some(e))
        if (resultStatistics.transformationStages.length == 0) {
          // No podemos dejar vacío el array de etapas de transformación porque luego no lo inserta bien en la tabla de estadísticas, por lo que le metemos un dummy
          val t = new Timestamp(System.currentTimeMillis())
          resultStatistics = resultStatistics.copy(
            transformationStages = List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(t, t, 0)))
          )
        }
        resultStatistics.copy(resultMsg = s"$msg\n${e.getMessage}")
    }
  }

  /**
   * Adds the parameters received into the dataframe as literal columns
   *
   * @param dataframe : Dataframe where parameter columns will be added
   * @return: Dataframe with parameter columns added
   */
  protected def addParameterColumns(dataframe: DataFrame): DataFrame = {

    var dfWithParameters = dataframe

    parameters.getOptions.foreach(param => {
      if (!dfWithParameters.columns.contains(param.getLongOpt)) {
        dfWithParameters =
          dfWithParameters
            .withColumn(
              param.getLongOpt,
              lit(param.getValue)
            )
      }
    })
    dfWithParameters
  }

  /**
   * Removes the parameters from the dataframe
   *
   * @param dataframe : Dataframe where parameter columns must be dropped
   * @return: Dataframe without the parameter columns
   */
  protected def dropParameterColumns(dataframe: DataFrame): DataFrame = {
    var dfWithoutParameters = dataframe
    parameters.getOptions.foreach(param => {
      dfWithoutParameters =
        dfWithoutParameters.drop(col(param.getLongOpt))
    })
    dfWithoutParameters
  }

  /**
   * Adds into the dataframe the standard storage info columns
   *
   * @param dataframe : input dataframe
   * @param uuid      : uuid to be added
   * @return: dataframe with the storage info columns
   */
  protected def addStorageInfoColumns(
                                       dataframe: DataFrame,
                                       uuid: String): DataFrame = {


    val unix_timestamp: Long = System.currentTimeMillis
    val dataLakeStorageDate = DateUtils.unixToDateTime(unix_timestamp)

    val validRowsWithStorageDate =
      dataframe
        .withColumn(
          GoldConstants.datalakeLoadDateFieldName,
          to_timestamp(
            lit(dataLakeStorageDate),
            "yyyy-MM-dd HH:mm:ss"
          ).cast("timestamp"))

    val validRowsWithStorageDay =
      validRowsWithStorageDate
        .withColumn(
          GoldConstants.datalakeLoadDayFieldName,
          date_trunc("Day", current_timestamp())
        )

    val validRowsWithIndationUuid =
      validRowsWithStorageDay.
        withColumn(
          GoldConstants.datalakeIngestionUuidFieldName,
          lit(uuid)
        )

    validRowsWithIndationUuid
  }

}
