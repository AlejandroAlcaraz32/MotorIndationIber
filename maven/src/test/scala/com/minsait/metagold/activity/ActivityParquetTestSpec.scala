package com.minsait.metagold.activity

import com.minsait.common.utils.BuildInfo
import com.minsait.metagold.activity.statistics.models.ActivityResults.{Fail, Success}
import com.minsait.metagold.activity.statistics.models.ActivityTriggerTypes.Adf
import com.minsait.metagold.activity.statistics.models._
import com.minsait.metagold.helpers.TestingHelper
import com.minsait.metagold.helpers.TestingHelper.{schemaDatamart, tableAggProduct}
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.DataHelper.readSQLTable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import scala.collection.mutable

class ActivityParquetTestSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  var args1: Array[String] = _
  var args2: Array[String] = _
  var args3: Array[String] = _
  var args4: Array[String] = _
  var args5: Array[String] = _
  var argsWrong: Array[String] = _

  override def beforeAll(): Unit = {
    args1 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testActivity",
      "--config-file", TestingHelper.pathToTestConfigParquet,
      "--paramCategory", "2")

    args2 = Array(
      "--adf-runid", "-999",
      "--transformation-activity", "testActivity",
      "--config-file", TestingHelper.pathToTestConfigParquet,
      "--paramCategory", "2")

    argsWrong = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "wrongTestActivity",
      "--config-file", TestingHelper.pathToTestConfigParquet,
      "--paramCategory", "2")

    args3 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testActivityQuality",
      "--config-file", TestingHelper.pathToTestConfigQ1Parquet,
      "--paramCategory", "2")

    args4 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testActivityQualityDos",
      "--config-file", TestingHelper.pathToTestConfigQ2Parquet,
      "--paramCategory", "2")
    args5 = Array(
      "--adf-runid", "-77",
      "--transformation-activity", "testActivity",
      "--config-file", TestingHelper.pathToTestConfigQ3Parquet,
      "--paramCategory", "2")

    TestingHelper.InitSparkSessionParquet
    TestingHelper.PrepareTestData(TestingHelper.indationPropertiesParquet)
  }

  test("ActivityParquetTestSpec#Full activity execution") {

    // Execution
    com.minsait.metagold.Main.main(args1)

    // Table results assertion
    val dataframe = readSQLTable(TestingHelper.SQLConnection, Some(s"$schemaDatamart.$tableAggProduct"), None, TestingHelper.indationPropertiesParquet)
    val filas = dataframe.collect()
    assert(dataframe.count == 1)
    assert(dataframe.schema.length == 2)
    assert(filas(0).getString(0) == "HL Road Frame - Black, 58")
    assert(filas(0)(1) == 1)
  }

  //TODO: Finalizar este test
  //  test("ActivityParquetTestSpec#Wrong activity execution") {
  //
  //    // Execution
  //    com.minsait.metagold.Main.main(argsWrong)
  //
  //  }

  test("ActivityParquetTestSpec#Statistics Json Parser Test") {
    import com.minsait.metagold.activity.statistics.ActivityStatsJsonProtocol._
    import spray.json._

    val activityStage = ActivityTransformationStage(
      "stage1",
      "tipoStage",
      Success,
      1,
      ActivityDuration(new Timestamp(0), new Timestamp(1), 1)
    )
    val activityTransform = ActivityTransformation(
      "transform1",
      "tipoTransform",
      Fail,
      ActivityDuration(new Timestamp(0), new Timestamp(1), 1),
      List(activityStage),
      "mensaje",
      List("")
    )

    var activityStatistics =
      ActivityStatistics(
        "uuid",
        ActivityTrigger(Adf, "-111"),
        ActivityEngine(BuildInfo.name, BuildInfo.version),
        ActivityExecution("activity", List(ActivityParameter("param1", "valor1"))),
        Success,
        ActivityDuration(new Timestamp(0), new Timestamp(1), 1),
        List(activityTransform),
        "")
    assert(!activityStatistics.toJson.toString().isEmpty)
  }

  test("ActivityParquetTestSpec#Statistics Writer Test") {

    // Execution
    com.minsait.metagold.Main.main(args2)

    // Read Statistics
    val dataframe = spark.table("aplicaciones.transform")

    assert(!dataframe.isEmpty)
  }

  test("ActivityParquetTestSpec#Full activity execution with Quality has 1 that does not satisfity quality") {
    // Execution
    com.minsait.metagold.Main.main(args3)

    val table = "aplicaciones.quality1"
    val dfParquet = spark.table(table)
    val stat1 = dfParquet.select(col("transformations").getItem(0)).collect()(0)(0)
    val stat2 = dfParquet.select(col("transformations").getItem(1)).collect()(0)(0)
    val stat3 = dfParquet.select(col("transformations").getItem(2)).collect()(0)(0)

    val actual1 = stat1.asInstanceOf[GenericRowWithSchema].get(1)
    val actual2 = stat2.asInstanceOf[GenericRowWithSchema].get(1)
    val actual3 = stat3.asInstanceOf[GenericRowWithSchema].get(1)

    val expected1 = mutable.WrappedArray.make(Array(""))
    val expected2 = mutable.WrappedArray.make(Array("validateQuality Gold: Data associated with transformation " +
      "category-quality-dlk has 1 rows which do not satisfy the expression ProductCategoryKey > 1"))
    val expected3 = mutable.WrappedArray.make(Array(""))

    assert(actual1 == expected1)
    assert(actual2 == expected2)
    assert(actual3 == expected3)

  }

  test("ActivityParquetTestSpec#Full activity execution with Quality hast 2 rows that do not satisfity quality") {
    // Execution
    com.minsait.metagold.Main.main(args4)

//    val path = "target/test-classes/metagold/datalake/gold/private/aplicaciones/quality2"
//    val stat1 = spark.read.parquet(path).select(col("transformations").getItem(0)).collect()(0)(0)
//    val stat2 = spark.read.parquet(path).select(col("transformations").getItem(1)).collect()(0)(0)
//    val stat3 = spark.read.parquet(path).select(col("transformations").getItem(2)).collect()(0)(0)
    val table = "aplicaciones.quality2"
    val dfParquet = spark.table(table)
    val stat1 = dfParquet.select(col("transformations").getItem(0)).collect()(0)(0)
    val stat2 = dfParquet.select(col("transformations").getItem(1)).collect()(0)(0)
    val stat3 = dfParquet.select(col("transformations").getItem(2)).collect()(0)(0)


    val actual1 = stat1.asInstanceOf[GenericRowWithSchema].get(1)
    val actual2 = stat2.asInstanceOf[GenericRowWithSchema].get(1)
    val actual3 = stat3.asInstanceOf[GenericRowWithSchema].get(1)

    val expected1 = mutable.WrappedArray.make(Array(""))
    val expected2 = mutable.WrappedArray.make(Array("validateQuality Gold: Data associated with transformation " +
      "category-qualitydos-dlk has 2 rows which do not satisfy the expression ProductCategoryKey > 2"))
    val expected3 = mutable.WrappedArray.make(Array(""))

    assert(actual1 == expected1)
    assert(actual2 == expected2)
    assert(actual3 == expected3)
  }
  test("ActivityParquetTestSpec#Full activity execution without quality should return empty values") {
    // Execution
    com.minsait.metagold.Main.main(args5)

//    val path = "target/test-classes/metagold/datalake/gold/private/aplicaciones/quality3"
//    val stat1 = spark.read.parquet(path).select(col("transformations").getItem(0)).collect()(0)(0)
//    val stat2 = spark.read.parquet(path).select(col("transformations").getItem(1)).collect()(0)(0)
//    val stat3 = spark.read.parquet(path).select(col("transformations").getItem(2)).collect()(0)(0)
    val table = "aplicaciones.quality3"
    val dfParquet = spark.table(table)
    val stat1 = dfParquet.select(col("transformations").getItem(0)).collect()(0)(0)
    val stat2 = dfParquet.select(col("transformations").getItem(1)).collect()(0)(0)
    val stat3 = dfParquet.select(col("transformations").getItem(2)).collect()(0)(0)

    val actual1 = stat1.asInstanceOf[GenericRowWithSchema].get(1)
    val actual2 = stat2.asInstanceOf[GenericRowWithSchema].get(1)
    val actual3 = stat3.asInstanceOf[GenericRowWithSchema].get(1)

    val expected1 = mutable.WrappedArray.make(Array(""))
    val expected2 = mutable.WrappedArray.make(Array(""))
    val expected3 = mutable.WrappedArray.make(Array(""))

    assert(actual1 == expected1)
    assert(actual2 == expected2)
    assert(actual3 == expected3)
  }

  //TODO: Añadir más tests

}
