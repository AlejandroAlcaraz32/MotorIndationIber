package com.minsait.metagold.activity


import com.minsait.indation.activity.{IngestJdbcTableActivity, IngestStreamingTopicActivity}
import com.minsait.indation.metadata.models.enums.DatasetTypes
import com.minsait.indation.metadata.models.enums.DatasetTypes.DatasetType
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.metagold.logging.Logging
import com.minsait.metagold.metadata.models._
import com.minsait.metagold.metadata.models.enums.AggregationTypes._
import com.minsait.metagold.metadata.models.enums.CalculationTypes.Expr
import com.minsait.metagold.metadata.models.enums.FilterTypes.FilterExpr
import com.minsait.metagold.metadata.models.enums.StageTypes._
import com.minsait.metagold.metadata.models.enums.MonthlyTypes._
import com.minsait.metagold.metadata.models.enums.FillerInputTypes._
import com.minsait.metagold.metadata.models.enums.ColsTypes.{MultipleCols, SingleCol}
import com.minsait.metagold.metadata.models.enums.TableTypes.{Gold, SQL, Silver, Test}
import com.minsait.common.spark.SparkSessionFactory.{persistedDataframes, temporaryViews}
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.DataHelper.{readGoldTable, readSilverTable}
import com.minsait.common.utils.SilverConstants
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.ml.feature._



object DlkStageProcess
  extends SparkSessionWrapper   with Logging {

  def execute(
               stage: DatalakeTransformationStage,
               dataFrame: DataFrame,
               uuid: String,
               metagoldProperties: IndationProperties
             ): DataFrame = {

    val startTimeMillis = System.currentTimeMillis

    this.logger.info(s"Starting gold datalake stage: ${stage.name}")

    var outputDf = stage.typ match {

      case Table =>{
        logger.info(s"Stage Table ${stage.stageTable.get.table}")
        processTableStage(stage.stageTable.get,dataFrame, uuid, metagoldProperties )
      }

      case CalculatedColumn =>{
        logger.info(s"Stage Calculated column ${stage.stageColumn.get.name}")
        processColumnStage(stage.stageColumn.get,dataFrame,uuid)
      }

      case Aggregation =>{
        logger.info(s"Stage Aggregation ${stage.stageAggregation.get.name}")
        processAggregationStage(stage.stageAggregation.get,dataFrame,uuid)
      }

      case Select =>{
        logger.info(s"Stage Select ${stage.stageSelect.get.map(c=>c.columnName).mkString(",")}")
        val columns = stage.stageSelect.get.map(c=>col(c.columnName).alias(c.columnAlias))
        dataFrame.select(columns:_*)
      }

      case Filter =>{
        logger.info(s"Stage Filter ${stage.name}: ${stage.stageFilter.get.filterExpr.get}")
        processFilterStage(stage.stageFilter.get,dataFrame,uuid)
      }

      case Filler => {
        logger.info(s"Stage Filler ${stage.stageFiller.get.name}")
        processFillerStage(stage.stageFiller.get, dataFrame, uuid)
      }

      case BinarizerTransform => {
        logger.info(s"Stage Binarizer ${stage.name}: ${stage.stageBinarizer.get.typ.value}")
        processBinarizerStage(stage.stageBinarizer.get, dataFrame, uuid)
      }

      case BucketizerTransform => {
        logger.info(s"Stage Bucketizer ${stage.name}: ${stage.stageBucketizer.get.typ.value}")
        processBucketizerStage(stage.stageBucketizer.get, dataFrame, uuid)
      }

      case VectorIndexerTransform => {
        logger.info(s"Stage Vector Indexer ${stage.name}: ${stage.stageVectorIndexer.get.inputCol.get}")
        processVectorIndexerStage(stage.stageVectorIndexer.get, dataFrame, uuid, metagoldProperties)
      }

      case OneHotEncoderTransform => {
        logger.info(s"Stage One Hot Encoder ${stage.name}: ${stage.stageOneHotEncoder.get.typ.value}")
        processOneHotEncoderStage(stage.stageOneHotEncoder.get, dataFrame, uuid, metagoldProperties)
      }

      case ImputerTransform => {
        logger.info(s"Stage Imputer ${stage.name}: ${stage.stageImputer.get.strategy.get} to ${stage.stageImputer.get.typ.value} cols")
        processImputerStage(stage.stageImputer.get, dataFrame, uuid, metagoldProperties)
      }

      case VariationTransform => {
        logger.info(s"Stage Variation ${stage.name}")
        processVariationStage(stage.stageVariation.get, dataFrame, uuid, metagoldProperties)
      }

      case MonthlyConverter => {
        logger.info(s"Stage MonthlyConverter ${stage.name}")
        processMonthlyConverter(stage.stageMonthlyConverter.get, dataFrame, uuid)
      }


      case other => {
        throw new UnsupportedOperationException(s"Stage ${stage.name} failed. Datalake Stage type not defined: $other. UUID: $uuid")
      }
    }

    //Drop
    if (stage.drop.isDefined) {
      outputDf = outputDf.drop(stage.drop.get: _*)
    }

    // Distinct
    if (stage.distinct.isDefined && stage.distinct.get){
      logger.info(s"Applying DISTINCT in stage ${stage.name}")
      outputDf = outputDf.distinct()
    }

    // Temp View if defined
    if (stage.tempView.isDefined){
      logger.info(s"Creating TEMP VIEW ${stage.tempView.get} in stage ${stage.name}")
      outputDf.createOrReplaceTempView(stage.tempView.get)
      temporaryViews = temporaryViews ++ List(stage.tempView.get)
    }

    // Cache if defined
    if (stage.cache.isDefined && stage.cache.get){
      logger.info(s"Applying CACHE in stage ${stage.name}")
      outputDf.cache()
      persistedDataframes = persistedDataframes ++ List(outputDf)
    }

    //TODO: Registrar estadísticas de etapa

    // Salida final
    outputDf

  }

  def processTableStage(
                         stageTable: StageTable,
                         dataFrame: DataFrame,
                         uuid: String,
                         metagoldProperties: IndationProperties
                       ): DataFrame = {

    // Referenced table from silver or gold
    var tableDataFrame = spark.emptyDataFrame

    stageTable.typ match {

      case Silver =>
        tableDataFrame=readSilverTable(
          metagoldProperties,
          s"${stageTable.database.get}.${stageTable.table}"
        )

      case Gold =>
        tableDataFrame = readGoldTable(
          metagoldProperties,
          s"${stageTable.classification.get}/${stageTable.database.get}/${stageTable.table}"
        )

      case SQL =>
        tableDataFrame = spark.sql(stageTable.sql.get)

      case Test =>
        tableDataFrame = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/${stageTable.database.get}/${stageTable.table}")

      case other =>
        throw new UnsupportedOperationException(s"Unsupported stage table type: $other")
    }

    // Table alias
    tableDataFrame = tableDataFrame.alias(stageTable.name)

    // If join expr defined, join table with previous table
    if (stageTable.joinExpr.isDefined) {
      dataFrame
        .join(
          tableDataFrame,
          expr(stageTable.joinExpr.get.expr),
          stageTable.joinExpr.get.typ.value
        )
    }
    else
    // if not join expr defined, the referenced table is returned, losing original dataframe.
      tableDataFrame
  }

  def processColumnStage(
                          stageColumn: StageColumn,
                          dataFrame: DataFrame,
                          uuid: String
                        ): DataFrame = {
    stageColumn.typ match {
      case Expr =>
        dataFrame.withColumn(stageColumn.name,expr(stageColumn.calculationExpr.get))
      case other =>
        throw new UnsupportedOperationException(s"Unsupported calculation type for stage column: ${stageColumn.typ}. UUID: $uuid")
    }
  }

  def processFilterStage(
                          stageFilter: StageFilter,
                          dataFrame: DataFrame,
                          uuid: String
                        ): DataFrame = {
    stageFilter.typ match {
      case FilterExpr =>
        dataFrame.where(expr(stageFilter.filterExpr.get))
      case other =>
        throw new UnsupportedOperationException(s"Unsupported filter type : $other. UUID: $uuid")
    }
  }

  def processFillerStage(
                          stageFiller: StageFiller,
                          dataFrame: DataFrame,
                          uuid: String
                        ): DataFrame = {
    stageFiller.typ match {
      case Btype => {
        logger.info(s"Stage fill nas with value ${stageFiller.value}")
        if (stageFiller.cols.isDefined) {
          dataFrame.na.fill(stageFiller.value.toBoolean, stageFiller.cols.get)
        } else {
          dataFrame.na.fill(stageFiller.value.toBoolean)
        }
      }
      case Ltype => {
        logger.info(s"Stage fill nas with value ${stageFiller.value}")
        if (stageFiller.cols.isDefined) {
          dataFrame.na.fill(stageFiller.value.toLong, stageFiller.cols.get)
        } else {
          dataFrame.na.fill(stageFiller.value.toLong)
        }
      }
      case Itype => {
        logger.info(s"Stage fill nas with value ${stageFiller.value}")
        if (stageFiller.cols.isDefined) {
          dataFrame.na.fill(stageFiller.value.toInt, stageFiller.cols.get)
        } else {
          dataFrame.na.fill(stageFiller.value.toInt)
        }
      }
      case Dtype => {
        logger.info(s"Stage fill nas with value ${stageFiller.value}")
        if (stageFiller.cols.isDefined) {
          dataFrame.na.fill(stageFiller.value.toDouble, stageFiller.cols.get)
        } else {
          dataFrame.na.fill(stageFiller.value.toDouble)
        }
      }
      case Stype => {
        logger.info(s"Stage fill nas with value ${stageFiller.value}")
        if (stageFiller.cols.isDefined) {
          dataFrame.na.fill(stageFiller.value, stageFiller.cols.get)
        } else {
          dataFrame.na.fill(stageFiller.value)
        }
      }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported binarizer type : $other. UUID: $uuid")
    }

  }

  def processBinarizerStage(
                             stageBinarizer: StageBinarizer,
                             dataFrame: DataFrame,
                             uuid: String
                           ): DataFrame = {
    stageBinarizer.typ match {
      case SingleCol => {
        logger.info(s"Stage single col: ${stageBinarizer.inputCol.get} with threshold ${stageBinarizer.threshold.get}")
        val binarizer: Binarizer = new Binarizer()
          .setInputCol(stageBinarizer.inputCol.get)
          .setOutputCol(stageBinarizer.outputCol.get)
          .setThreshold(stageBinarizer.threshold.get)

        binarizer.transform(dataFrame)

      }
      case MultipleCols => {
        logger.info(s"Stage multiple cols: ${stageBinarizer.inputCols.get} with thresholds ${stageBinarizer.thresholds.get}")
        val binarizer: Binarizer = new Binarizer()
          .setInputCols(stageBinarizer.inputCols.get)
          .setOutputCols(stageBinarizer.outputCols.get)
          .setThresholds(stageBinarizer.thresholds.get)

        binarizer.transform(dataFrame)
      }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported binarizer type : $other. UUID: $uuid")
    }
  }

  def processBucketizerStage(
                              stageBucketizer: StageBucketizer,
                              dataFrame: DataFrame,
                              uuid: String
                            ): DataFrame = {
    stageBucketizer.typ match {
      case SingleCol => {
        logger.info(s"Stage single col: ${stageBucketizer.inputCol.get} with splits ${stageBucketizer.splits.get}")
        val bucketizer: Bucketizer = new Bucketizer()
          .setInputCol(stageBucketizer.inputCol.get)
          .setOutputCol(stageBucketizer.outputCol.get)
          .setSplits(stageBucketizer.splits.get)

        bucketizer.transform(dataFrame)

      }
      case MultipleCols => {
        logger.info(s"Stage multiple cols: ${stageBucketizer.inputCols.get} with splits array ${stageBucketizer.splitsArrays.get}")
        val bucketizer: Bucketizer = new Bucketizer()
          .setInputCols(stageBucketizer.inputCols.get)
          .setOutputCols(stageBucketizer.outputCols.get)
          .setSplitsArray(stageBucketizer.splitsArrays.get)

        bucketizer.transform(dataFrame)
      }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported binarizer type : $other. UUID: $uuid")
    }
  }

  def processVectorIndexerStage(
                                 stageVectorIndexer: StageVectorIndexer,
                                 dataFrame: DataFrame,
                                 uuid: String,
                                 metagoldProperties: IndationProperties
                               ): DataFrame = {

    var historicalDF = spark.emptyDataFrame
    historicalDF = processTableStage(stageVectorIndexer.historicalTable, historicalDF, uuid, metagoldProperties)
    logger.info(s"Hitorical table: ${stageVectorIndexer.historicalTable.table} read")

    val indexer = new VectorIndexer()
      .setInputCol(stageVectorIndexer.inputCol.get)
      .setOutputCol(stageVectorIndexer.outputCol.get)
      .setMaxCategories(stageVectorIndexer.maxCategories.get)

    // val indexerModel = indexer.fit(historicalDF)
    val indexerModel = indexer.fit(dataFrame)
    indexerModel.transform(dataFrame)

  }

  def processOneHotEncoderStage(
                                 stageOneHotEncoder: StageOneHotEncoder,
                                 dataFrame: DataFrame,
                                 uuid: String,
                                 metagoldProperties: IndationProperties
                               ): DataFrame = {

    var historicalDF = spark.emptyDataFrame
    historicalDF = processTableStage(stageOneHotEncoder.historicalTable, historicalDF, uuid, metagoldProperties)
    logger.info(s"Hitorical table: ${stageOneHotEncoder.historicalTable.table} read")

    stageOneHotEncoder.typ match {
      case SingleCol => {
        logger.info(s"Stage single col: ${stageOneHotEncoder.inputCol.get} ")
        val encoder: OneHotEncoder = new OneHotEncoder()
          .setInputCol(stageOneHotEncoder.inputCol.get)
          .setOutputCol(stageOneHotEncoder.outputCol.get)

        val encoderModel = encoder.fit(historicalDF)
        encoderModel.transform(dataFrame)

      }
      case MultipleCols => {
        logger.info(s"Stage multiple cols: ${stageOneHotEncoder.inputCols.get} ")
        val encoder: OneHotEncoder = new OneHotEncoder()
          .setInputCols(stageOneHotEncoder.inputCols.get)
          .setOutputCols(stageOneHotEncoder.outputCols.get)

        val encoderModel = encoder.fit(historicalDF)
        encoderModel.transform(dataFrame)
      }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported binarizer type : $other. UUID: $uuid")
    }
  }

  def processImputerStage(
                           stageImputer: StageImputer,
                           dataFrame: DataFrame,
                           uuid: String,
                           metagoldProperties: IndationProperties
                         ): DataFrame = {

    var historicalDF = spark.emptyDataFrame

    if (stageImputer.historicalTable.isDefined) {
      historicalDF = processTableStage(stageImputer.historicalTable.get, historicalDF, uuid, metagoldProperties)
      logger.info(s"Hitorical table: ${stageImputer.historicalTable.get.table} read")
    } else {
      historicalDF = dataFrame
    }


    stageImputer.typ match {
      case SingleCol => {
        logger.info(s"Stage single col: ${stageImputer.inputCol.get} ")

        val imputer: Imputer = new Imputer()
          .setInputCol(stageImputer.inputCol.get)
          .setOutputCol(stageImputer.outputCol.get)
          .setStrategy(stageImputer.strategy.get.value)

        val encoderModel = imputer.fit(historicalDF)
        encoderModel.transform(dataFrame)

      }
      case MultipleCols => {
        logger.info(s"Stage multiple cols: ${stageImputer.inputCols.get} ")
        val imputer: Imputer = new Imputer()
          .setInputCols(stageImputer.inputCols.get)
          .setOutputCols(stageImputer.outputCols.get)
          .setStrategy(stageImputer.strategy.get.value)

        val encoderModel = imputer.fit(historicalDF)
        encoderModel.transform(dataFrame)
      }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported binarizer type : $other. UUID: $uuid")
    }
  }

  def processVariationStage(
                             stageVariation: StageVariation,
                             dataFrame: DataFrame,
                             uuid: String,
                             metagoldProperties: IndationProperties
                           ): DataFrame = {

    val windowSpec = org.apache.spark.sql.expressions.Window.orderBy(stageVariation.timeOrderCol)

    if (stageVariation.percentage) {
      val leadDf = dataFrame.withColumn("new_col", lag(stageVariation.inputCol, stageVariation.timeLag, 0).over(windowSpec))
      leadDf.withColumn(stageVariation.outputCol, when(col("new_col") === 0, 0).otherwise((col({
        stageVariation.inputCol
      }) - col("new_col")) / col({
        stageVariation.inputCol
      }) * 100)).drop("new_col")
    }
    else {
      val leadDf = dataFrame.withColumn("new_col", lag(stageVariation.inputCol, stageVariation.timeLag, 0).over(windowSpec))
      leadDf.withColumn(stageVariation.outputCol, when(col("new_col") === 0, 0).otherwise(col({
        stageVariation.inputCol
      }) - col("new_col"))).drop("new_col")
    }
  }

  def processAggregationStage(
                               stageAggregation: StageAggregation,
                               dataFrame: DataFrame,
                               uuid: String
                             ): DataFrame = {
    //Group by
    val tagsForGroupBy = stageAggregation.stageGroupBy
    var groupedDataFrame=
      dataFrame.groupBy(
        tagsForGroupBy.head, tagsForGroupBy.tail:_*
      )

    //Get columns with aggregation expressions
    val columns = stageAggregation.aggregations.map(
      a=>getAggregation(a)
    )

    //Returns dataframe with aggregations
    groupedDataFrame
      .agg(
        columns.head, columns.tail:_*
      )
  }

  def getAggregation(aggregation: AggregationColumn): Column = {
    count(lit(1)).alias("alias")
    //TODO: revisar si hay un método mejor
    aggregation.typ match {
      case Sum =>
        sum(aggregation.col).alias(aggregation.name)
      case SumDistinct =>
        sumDistinct(aggregation.col).alias(aggregation.name)
      case Count =>
        count(aggregation.col).alias(aggregation.name)
      case CountDistinct =>
        countDistinct(aggregation.col).alias(aggregation.name)
      case Min =>
        min(aggregation.col).alias(aggregation.name)
      case Max  =>
        max(aggregation.col).alias(aggregation.name)
      case First =>
        first(aggregation.col).alias(aggregation.name)
      case Last  =>
        last(aggregation.col).alias(aggregation.name)
      case Avg  =>
        avg(aggregation.col).alias(aggregation.name)
      case Mean  =>
        mean(aggregation.col).alias(aggregation.name)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported aggregation type: $other.")
    }
  }

  def processMonthlyConverter(stageMonthlyConverter: StageMonthlyConverter,
                              dataFrame: DataFrame,
                              uuid: String
                             ): DataFrame = {
    var dataFrameOut = spark.emptyDataFrame
    var dfMax = spark.emptyDataFrame
    var n : Int = 0
    val campo  =  stageMonthlyConverter.dateCol
    val campoID  =  stageMonthlyConverter.idCol
    val dataFrameOrdered = dataFrame.orderBy(campo)

    stageMonthlyConverter.freq match {
      case Two =>
        n = 2
      case Three =>
        n = 3
      case Four =>
        n= 4
      case Six =>
        n = 5
      case other =>
        throw new UnsupportedOperationException(s"Unsupported frequency type: $other.")
    }

    // Cuando la diferencia entre el mes actual y el último mes no sigue la frecuencia determinada.
    val distinct_date = dataFrameOrdered.select(col(campo).cast(StringType), col(campoID)
    ).distinct.orderBy(campo
    ).withColumn("months", month(col(campo)))
    val months_diff = distinct_date.withColumn("months_lag", expr(s"lag(months, 1) over(PARTITION BY $campoID ORDER BY $campo)")
    ).na.fill(0).withColumn("months_lag", when(col("months_lag") === 12, 0).otherwise(col("months_lag"))
    ).withColumn("is_trim", col("months") - col("months_lag"))

    val dates = months_diff.where(expr(s"is_trim = $n")).orderBy(campo).distinct.collect.map(r=>r(0).toString)
    val dfMonthly = dataFrameOrdered.where(col(campo).isin(dates: _*))

    val diff = months_diff.where(expr(s"is_trim != $n")).orderBy(campo).agg(last(campo), last("is_trim")).collect

    if (!diff.isEmpty) {
      val last_date = diff(0).getString(0)
      val num = diff(0).getInt(1)

      dfMax = dataFrameOrdered.where(expr(s"$campo= '$last_date'"))
      if (num != 1) {
        for (i <- 1 until num) {
          val df_i = dfMax.withColumn(campo, add_months(col(campo), -i))
          dfMax = dfMax.union(df_i)
        }
      }
    }

    stageMonthlyConverter.freq match {
      case Two =>
        val fecha_post = dfMonthly.withColumn(campo, add_months(col(campo), -1))
        dataFrameOut = fecha_post.union(dfMonthly)
      case Three =>
        val fecha_post = dfMonthly.withColumn(campo, add_months(col(campo), -1))
        val fecha_post2 = dfMonthly.withColumn(campo, add_months(col(campo), -2))
        dataFrameOut = fecha_post.union(fecha_post2).union(dfMonthly)
      case Four =>
        val fecha_post = dfMonthly.withColumn(campo, add_months(col(campo), -1))
        val fecha_post2 = dfMonthly.withColumn(campo, add_months(col(campo), -2))
        val fecha_post3 = dfMonthly.withColumn(campo, add_months(col(campo), -3))
        dataFrameOut = fecha_post.union(fecha_post2).union(fecha_post3).union(dfMonthly)
      case Six =>
        val fecha_post = dfMonthly.withColumn(campo, add_months(col(campo), -1))
        val fecha_post2 = dfMonthly.withColumn(campo, add_months(col(campo), -2))
        val fecha_post3 = dfMonthly.withColumn(campo, add_months(col(campo), -3))
        val fecha_post4 = dfMonthly.withColumn(campo, add_months(col(campo), -4))
        val fecha_post5 = dfMonthly.withColumn(campo, add_months(col(campo), -5))
        dataFrameOut = fecha_post.union(fecha_post2).union(fecha_post3).union(fecha_post4).union(fecha_post5).union(dfMonthly)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported frequency type: $other.")
    }

    if (dfMax.rdd.isEmpty) {
      dataFrameOut.orderBy(campo)
    }
    else {
      dataFrameOut.union(dfMax).orderBy(campo)
    }
  }

}
