package com.minsait.metagold.activity

import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.minsait.metagold.helpers.TestingHelper
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.metagold.Main
import com.minsait.metagold.metadata.models.StageFiller
import com.minsait.metagold.metadata.models.enums.FillerInputTypes.{Btype, Dtype, FillerInputTypes, Itype, Ltype, Stype}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import com.minsait.metagold.metadata.models.StageMonthlyConverter
import com.minsait.metagold.metadata.models.enums.MonthlyTypes.MonthlyTypes

import java.io.File
import scala.reflect.io.Directory


class ActivityTestML extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  var args0: Array[String] = _
  var args1: Array[String] = _
  var args2: Array[String] = _
  var args3: Array[String] = _
  var args4: Array[String] = _
  var args5: Array[String] = _
  var args6: Array[String] = _
  var argsWrong: Array[String] = _

  override def beforeAll(): Unit = {

    args0 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity0",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    args1 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity1",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    args2 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity2",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    args3 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity3",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    args4 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity4",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    args5 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity5",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    args6 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testMLActivity6",
      "--paramCMESANO", "today",
      "--config-file", TestingHelper.pathToTestConfigDelta)


    TestingHelper.InitSparkSessionDelta
  }

  test("Read Table") {
    // Execution
    val directory = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_0"))
    //directory.deleteRecursively()

    Main.main(args0)

    val tableDataFrame_result = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_0").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")
    val tableDataFrame_val = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_0").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff = tableDataFrame_result.except(tableDataFrame_val)

    assert(tableDataFrame_diff.count == 0)
  }

  test("Column + Select + Aggregation + Join") {
    // Execution
    val directory_1a = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_1a"))
    val directory_1b = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_1b"))
   // directory_1a.deleteRecursively()
    // directory_1b.deleteRecursively()

    Main.main(args1)

    val tableDataFrame_result_1a = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_1a").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")
    val tableDataFrame_val_1a = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_1a").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff_1a = tableDataFrame_result_1a.except(tableDataFrame_val_1a)

    assert(tableDataFrame_diff_1a.count == 0)

    val tableDataFrame_result_1b = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_1b").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")
    val tableDataFrame_val_1b = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_1b").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff_1b = tableDataFrame_result_1b.except(tableDataFrame_val_1b)

    assert(tableDataFrame_diff_1b.count == 0)
  }

  test("ML Transformation") {
    // Execution
    val directory = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_2"))
    //directory.deleteRecursively()

    Main.main(args2)

    val tableDataFrame_result = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_2").orderBy("int").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")
    val tableDataFrame_val = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_2").orderBy("int").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff = tableDataFrame_result.except(tableDataFrame_val)

    assert(tableDataFrame_diff.count == 0)
  }

  test("ML Transformation with historical tables") {
    // Execution
    val directory = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_3"))
   // directory.deleteRecursively()

    Main.main(args3)

    val tableDataFrame_result = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_3").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")
    val tableDataFrame_val = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_3").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff = tableDataFrame_result.except(tableDataFrame_val)

    assert(tableDataFrame_diff.count == 0)
  }

  test("ML Transformation with params") {
    // Execution
    val directory = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_4"))
   // directory.deleteRecursively()

    Main.main(args4)

    val tableDataFrame_result = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_4").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")
    val tableDataFrame_val = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_4").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff = tableDataFrame_result.except(tableDataFrame_val)

    assert(tableDataFrame_diff.count == 0)
  }

  test("Fill na and drop columns") {
    // Execution
    val directory = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_5"))
    //directory.deleteRecursively()

    Main.main(args5)

    val tableDataFrame_result = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_5").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_val = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_5").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff = tableDataFrame_result.except(tableDataFrame_val)

    assert(tableDataFrame_diff.count == 0)
  }

  test("cmesano") {
    // Execution
    val directory = new Directory(new File("src/test/resources/metagold/datalake/gold/data_output/test_6"))
   // directory.deleteRecursively()

    Main.main(args6)

    val tableDataFrame_result = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/gold/data_output/test_6").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_val = spark.read.format("delta").load(s"src/test/resources/metagold/datalake/data_validation/test_6").drop("gold_load_date", "gold_load_day", "gold_ingestion_uuid")

    val tableDataFrame_diff = tableDataFrame_result.except(tableDataFrame_val)

    assert(tableDataFrame_diff.count == 0)
  }

  test("ActivityDeltaTestSpec#processFillerStage returns dataframes filled based on type") {

    //Creamos stageFillers en funciónd el tipo de dato
    val fillerBoolean = StageFiller("Boolean Filler", "d", FillerInputTypes(Btype.value), "true", Some(Array("TIPO_BOOLEANO")))
    val fillerInteger = StageFiller("Integer Filler", "d", FillerInputTypes(Itype.value), "25", Some(Array("TIPO_ENTERO")))
    val fillerLong = StageFiller("Long Filler", "d", FillerInputTypes(Ltype.value), "500", Some(Array("TIPO_LONG")))
    val fillerDouble = StageFiller("Double Filler", "d", FillerInputTypes(Dtype.value), "2.5", Some(Array("TIPO_DOUBLE")))
    val fillerString = StageFiller("Double Filler", "d", FillerInputTypes(Stype.value), "reemplazo", Some(Array("TIPO_STRING")))

    //Creamos la data con los diferentes tipos
    val simpleSchema = StructType(Array(
      StructField("TIPO_BOOLEANO",BooleanType,true),
      StructField("TIPO_ENTERO",IntegerType,true),
      StructField("TIPO_LONG",LongType,true),
      StructField("TIPO_DOUBLE",DoubleType,true),
      StructField("TIPO_STRING",StringType,true)
    ))
    val simpledata = Seq(
      Row(true, 1,200L,1.5,"string1"),
      Row(false, 2,200L,2.5,"string2"),
      Row(true, 3,400L,3.5,"string3"),
      Row(null, null,null,null,null))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpledata),simpleSchema)

    //Usamos el processFillerStage
    val outputBool: DataFrame = DlkStageProcess.processFillerStage(fillerBoolean, df, "uuid")
    val outputInteger :DataFrame = DlkStageProcess.processFillerStage(fillerInteger,df,"uuid")
    val outputLong :DataFrame = DlkStageProcess.processFillerStage(fillerLong,df,"uuid")
    val outputDouble:DataFrame = DlkStageProcess.processFillerStage(fillerDouble,df,"uuid")
    val outputString :DataFrame = DlkStageProcess.processFillerStage(fillerString,df,"uuid")

    //Comprobamos que no hay nulos
    assert(outputBool.filter(col("TIPO_BOOLEANO").isNull).count() == 0)
    assert(outputInteger.filter(col("TIPO_ENTERO").isNull).count() == 0)
    assert(outputLong.filter(col("TIPO_LONG").isNull).count() == 0)
    assert(outputDouble.filter(col("TIPO_DOUBLE").isNull).count() == 0)
    assert(outputString.filter(col("TIPO_STRING").isNull).count() == 0)

    //Comprobamos los reemplazos
    assert(outputBool.select(last(col("TIPO_BOOLEANO"))).collect()(0)(0) == true)
    assert(outputInteger.select(last(col("TIPO_ENTERO"))).collect()(0)(0) == 25)
    assert(outputLong.select(last(col("TIPO_LONG"))).collect()(0)(0) == 500)
    assert(outputDouble.select(last(col("TIPO_DOUBLE"))).collect()(0)(0) == 2.5)
    assert(outputString.select(last(col("TIPO_STRING"))).collect()(0)(0) == "reemplazo")

  }

  test ("MonthlyConverter"){

    import spark.implicits._

    val simpleSchema = StructType(Array(
      StructField("dateCol",StringType,true),
      StructField("idCol",StringType,true)))
    val simpledata =
      Seq(
        Row("2020-07-15", "1"),
        Row("2020-06-20", "1"),
        Row("2020-09-25", "1"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpledata),simpleSchema)

       val dfEsperado: DataFrame = Seq(("2020-07-15", "1"),
      ("2020-08-25", "1"),
      ("2020-09-25", "1")
    ).toDF("dateCol", "idCol")


    val monthlyTypes = MonthlyTypes("bi-monthly")

    val Stage= StageMonthlyConverter("Stagetest", "testMontlyConverter", monthlyTypes, "dateCol", "idCol")

    val DlkStageProcess = com.minsait.metagold.activity.DlkStageProcess
    val dfFinal = DlkStageProcess.processMonthlyConverter(Stage, df, "-99" )

    val dfFinal_compare= dfFinal.select("dateCol").collect()
    val dfEsperado_compare = dfEsperado.select("dateCol").collect()

    assert(dfFinal_compare===dfEsperado_compare)
  }

}
