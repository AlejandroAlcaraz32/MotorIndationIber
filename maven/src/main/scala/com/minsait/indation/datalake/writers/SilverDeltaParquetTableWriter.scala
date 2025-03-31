package com.minsait.indation.datalake.writers

import com.minsait.common.configuration.models.DatalakeOutputTypes.Delta
import com.minsait.common.configuration.models.{IndationProperties, SecurityEncryptionTypes}
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.EncryptUtils
import com.minsait.common.utils.EncryptUtils.{encryptUDF, stringKeyGenerator}
import com.minsait.indation.datalake.DatalakeManager
import com.minsait.indation.metadata.models.Dataset
import com.minsait.indation.metadata.models.enums.IngestionTypes
import com.minsait.indation.metalog.models.enums.ReprocessTypes
import com.minsait.indation.silver.helper.SchemaHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, concat, lit, sha2}
import org.apache.spark.sql.types.BinaryType

object SilverDeltaParquetTableWriter extends SilverTableWriter with SparkSessionWrapper {

  var indationProperties: IndationProperties = _

  //TODO: incluir tratamiento del Option de salida referente a los datos sensibles cifrados, en aquellos lugares donde se llama a writeSilver
  override def writeSilver(indationProperties: IndationProperties,
                           fileInputPath: String,
                           dataframe: DataFrame,
                           dataset: Dataset, uuid: String,
                           reprocessType: ReprocessTypes.ReprocessType): (SilverPersistenceWriteInfo, Option[SilverPersistenceWriteInfo]) = {

    this.indationProperties=indationProperties

    //Aqui tenemos que quedarnos solo con las columnas con ignorePersistence a false,
    //y despues cambiar los nombres a las columnas con alias
    //Se hace antes de incluir las columnas de info, para que despues se incluyan si o si

    val persistentAndAliasDataframe = persistentAndAliasColumns(dataset, dataframe)

    val (primaryKeyColumns, timeStampColumn, sensitiveColumns) = getPrimaryTimestampSensitiveColumnNames(dataset)

    if(indationProperties.security.encryption.encryptionType!= SecurityEncryptionTypes.None && sensitiveColumns.nonEmpty) {

      val (sensitivePersistentAndAliasDataFrame: DataFrame, nonSensitivePersistentAndAliasDataFrame: DataFrame) = pseudonymizeEncryptAndSplit(persistentAndAliasDataframe,primaryKeyColumns,sensitiveColumns, timeStampColumn)
      val sensitiveDataframeToWrite = this.addStorageInfoColumns(fileInputPath, sensitivePersistentAndAliasDataFrame, dataset, uuid)
      val nonSensitiveDataframeToWrite = this.addStorageInfoColumns(fileInputPath, nonSensitivePersistentAndAliasDataFrame, dataset, uuid)
      val nonSensitivePrimaryKeyColumns = primaryKeyColumns
      val sensitivePrimaryKeyColumns = Seq("_id").toList

      val datalakeManager = new DatalakeManager(indationProperties)

      val sensitiveSilverWritePath = datalakeManager.writeSilver(dataset.ingestionMode, dataset.database, dataset.table + "_sensitive", sensitiveDataframeToWrite
        , "", sensitivePrimaryKeyColumns, dataset.createDatabase, dataset.classification.value
        , timeStampColumn, reprocessType, dataset.getAllowPartitionChange,dataset.name)
      val NonSensitiveSilverWritePath = datalakeManager.writeSilver(dataset.ingestionMode, dataset.database, dataset.table, nonSensitiveDataframeToWrite
        , dataset.partitionBy, nonSensitivePrimaryKeyColumns, dataset.createDatabase, dataset.classification.value
        , timeStampColumn, reprocessType, dataset.getAllowPartitionChange,dataset.name)

      //TODO: cómo crear para dataframe sensitive ¿modificar las funciones?
      val (principalPreviousVersion, principalCurrentVersion) = this.principalTableVersions(dataset)
      val (historicalPreviousVersion, historicalCurrentVersion) = this.historicalTableVersions(dataset)

      (SilverPersistenceWriteInfo(NonSensitiveSilverWritePath, principalPreviousVersion, principalCurrentVersion, historicalPreviousVersion, historicalCurrentVersion),
      Some(SilverPersistenceWriteInfo(sensitiveSilverWritePath, principalPreviousVersion, principalCurrentVersion, historicalPreviousVersion, historicalCurrentVersion)))
    }
    else {
      //Primero añade las columnas de info

      val dataframeToWrite = this.addStorageInfoColumns(fileInputPath, persistentAndAliasDataframe, dataset, uuid)
      val datalakeManager = new DatalakeManager(indationProperties)

      val silverWritePath = datalakeManager.writeSilver(dataset.ingestionMode, dataset.database, dataset.table, dataframeToWrite
        , dataset.partitionBy, primaryKeyColumns, dataset.createDatabase, dataset.classification.value
        , timeStampColumn, reprocessType, dataset.getAllowPartitionChange,dataset.name)
      val (principalPreviousVersion, principalCurrentVersion) = this.principalTableVersions(dataset)
      val (historicalPreviousVersion, historicalCurrentVersion) = this.historicalTableVersions(dataset)

      (SilverPersistenceWriteInfo(silverWritePath, principalPreviousVersion, principalCurrentVersion, historicalPreviousVersion, historicalCurrentVersion),None)
    }
  }

  def persistentAndAliasColumns(dataset: Dataset, dataframe: DataFrame): DataFrame = {
    val columns = dataframe.columns
    val persistentColumns = SchemaHelper.persistentColumns(dataset)

    columns.foldLeft(dataframe)((newDataframe,column) =>{
      if(!persistentColumns.contains(column)) {
        newDataframe.drop(column)
      } else {
        persistentColumns(column) match {
          case Some(alias) => newDataframe.withColumnRenamed(column, alias)
          case None => newDataframe
        }
      }
    })
  }

  def getPrimaryTimestampSensitiveColumnNames(dataset: Dataset) : (List[String], String, List[String]) = {
    val persistentColumnsWithAlias = SchemaHelper.persistentColumns(dataset)
    val primaryKeyColumns = SchemaHelper.primaryKeys(dataset).map( pk => {
      //TODO: ¿Comprobar si el campo pk existe en el map de las columnas persistentes? si es pk y no persistente, debería ser error
      persistentColumnsWithAlias(pk) match {
        case Some(alias) => alias
        case None => pk
      }
    })
    var timeStampColumn = SchemaHelper.timeStampColumn(dataset)
    //TODO: Se comprueba si la columna timestamp existe en las persistentes, ya que si no hay columna timestamp, timeStampColumn vale "" y fallaría el recoger el valor del Map
    timeStampColumn = if(persistentColumnsWithAlias.contains(timeStampColumn)){
      persistentColumnsWithAlias(timeStampColumn) match {
        case Some(alias) => alias
        case None => timeStampColumn
      }
    } else {
      timeStampColumn
    }
    val sensitiveColumns = SchemaHelper.sensitiveColumns(dataset).map( sen => {
      //TODO: ¿Comprobar si el campo sen existe en el map de las columnas persistentes? si es sen y no persistente, debería ser error
      persistentColumnsWithAlias(sen) match {
        case Some(alias) => alias
        case None => sen
      }
    })
    (primaryKeyColumns,timeStampColumn, sensitiveColumns)
  }

  private def principalTableVersions(dataset: Dataset): (Option[Long], Option[Long]) = {
    this.tableVersions(dataset.database, dataset.table)
  }

  private def historicalTableVersions(dataset: Dataset): (Option[Long], Option[Long]) = {
    //if(dataset.ingestionMode == IngestionTypes.FullSnapshot || dataset.ingestionMode == IngestionTypes.LastChanges) {
    //  this.tableVersions(dataset.database, dataset.table + "_historical")
    //}
    //else {
    //  (None, None)
    //}
    (None, None)
  }

  private def tableVersions(database: String, table: String): (Option[Long], Option[Long]) = {
    import spark.implicits._

    import scala.collection.JavaConverters._

    if (indationProperties.datalake.outputType.get==Delta) {
      val principalLastChange = spark.sql(s"""DESCRIBE HISTORY $database.$table""")
        .as[DeltaTableHistory].collectAsList.asScala.toList.find(h => h.operation.isDefined && h.operation.get.equals("WRITE"))

      if (principalLastChange.isDefined) {
        (principalLastChange.get.readVersion, principalLastChange.get.version)
      } else {
        (None, None)
      }
    }
    else { // En caso de formato Parquet, no tenemos versiones de tabla
      (None, None)
    }
  }

  private def pseudonymizeEncryptAndSplit(initialDF: DataFrame, primaryKeyColumns: List[String], sensitiveColumns: List[String], timeStampColumn: String): (DataFrame, DataFrame) = {
    val key: String = stringKeyGenerator(256)
    val masterKey = EncryptUtils.getMasterKey

    val newIdColName: String = "_id"
    val saltColName: String = "hash_salt"
    val keyColName: String = "key"
    val map = Map[String, String](
      "algorithm" -> EncryptUtils.algorithm,
      "algorithm_transformation" -> EncryptUtils.algorithm_transformation,
      "keySalt" -> EncryptUtils.keySalt,
      "salt" -> EncryptUtils.salt
    )
    // spark.sparkContext.broadcast(map)

    val hashSalt: String = EncryptUtils.getHashSalt

    val initialDfWithNewIdAndKey: DataFrame = initialDF
      .withColumn(saltColName, lit(hashSalt))
      .withColumn(newIdColName, concat(primaryKeyColumns.map(col): _*))
      .withColumn(newIdColName, sha2(concat(col(newIdColName), col(saltColName)), 512))
      .withColumn(newIdColName, sha2(concat(col(newIdColName)), 512))
      .withColumn(keyColName, lit(key))

    val initialDfWithNewIdAndKeyColumnsList: List[String] = initialDfWithNewIdAndKey.columns.toList

    val sensitiveDF: DataFrame = initialDfWithNewIdAndKeyColumnsList.foldLeft(initialDfWithNewIdAndKey) ( (a,b) => {
      if (sensitiveColumns.contains(b)) {
        a
          .withColumn(s"encrypted_$b", encryptUDF(key,map)(coalesce(col(b).cast("string"),lit("null"))))
          .drop(col(b))
      }
      else if (b == keyColName) {
        a
          .withColumn(s"encrypted_$b", encryptUDF(masterKey,map,true)(col(b).cast("string")))
          .drop(col(b))
      }
      else if (b == newIdColName || b == timeStampColumn) a
      else a.drop(col(b))
    }).drop(col(saltColName))

    val nonSensitiveDF: DataFrame = initialDfWithNewIdAndKeyColumnsList.foldLeft(initialDfWithNewIdAndKey)( (a,b) => {
      if (sensitiveColumns.contains(b)) {
        val columnType = a.schema(b).dataType
        columnType match {
          case BinaryType => a.withColumn(b, sha2(concat(coalesce(col(b).cast("string"), lit("null")), col(saltColName)), 512) )
          case _ => a.withColumn(b, sha2(concat(coalesce(col(b).cast("string"), lit("null")), col(saltColName)).cast(BinaryType), 512) )
        }
      }
      else if (b == keyColName) a.drop(col(b))
      else a
    }).drop(col(saltColName))

    (sensitiveDF, nonSensitiveDF)
  }
}