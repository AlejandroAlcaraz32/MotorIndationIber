package com.minsait.metagold.activity

import com.minsait.common.configuration.models.DatalakeOutputTypes
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.common.utils.{BuildInfo, DataHelper}
import com.minsait.metagold.activity.statistics.models.ActivityResults.{Fail, Success}
import com.minsait.metagold.activity.statistics.models.ActivityTriggerTypes.Adf
import com.minsait.metagold.activity.statistics.models._
import com.minsait.metagold.helpers.TestingHelper
import com.minsait.metagold.helpers.TestingHelper.{cleanStats, schemaDatamart, tableAggProduct}
import com.minsait.common.utils.DataHelper.readSQLTable
import com.minsait.metagold.activity.exceptions.OutputTypeException
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class ActivityDeltaTestSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with SparkSessionWrapper {

  var args1: Array[String] = _
  var args2: Array[String] = _
  var argsWrong: Array[String] = _
  var argsTestOkWrong1: Array[String] = _
  var argsTestOkWrong2: Array[String] = _
  var argsTestWrongTransform: Array[String] = _
  var argsTestWrongActivity: Array[String] = _

  override def beforeAll(): Unit = {
    args1 = Array(
      "--adf-runid", "-1",
      "--transformation-activity", "testActivity",
      "--config-file", TestingHelper.pathToTestConfigDelta,
      "--paramCategory","2")

    args2 = Array(
      "--adf-runid", "-999",
      "--transformation-activity", "testActivity",
      "--config-file", TestingHelper.pathToTestConfigDelta,
      "--paramCategory","2")

    argsWrong = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "wrongTestActivity",
      "--config-file", TestingHelper.pathToTestConfigDelta,
      "--paramCategory","2")

    argsTestOkWrong1 = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "demoTransformOk",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    argsTestOkWrong2 = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "demoTransformError",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    argsTestWrongTransform = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "transformNotExists",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    argsTestWrongActivity = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "activityNotExists",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    TestingHelper.InitSparkSessionDelta
    TestingHelper.PrepareTestData(TestingHelper.indationPropertiesDelta)
  }


  test("ActivityDeltaTestSpec#Dependencies OK"){
    val arguments = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "dependenciesOk",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    // Execution
    com.minsait.metagold.Main.main(arguments)

    // Con que no genere error debería ser suficiente
  }

  test("ActivityDeltaTestSpec#Dependencies Circular Dependence"){
    val arguments = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "dependenciesCircular",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    // Limpiamos estadísticas
    cleanStats()

    // Execution
    try {
      com.minsait.metagold.Main.main(arguments)
      assert(false) //debería fallar antes
    }
    catch{
      case e:Throwable=>{
        val statistics = spark.sql("select * from aplicaciones.transform order by datalake_load_date desc")
        assert(statistics.count()==1)
        assert(statistics.select("resultMsg").collect()(0).getString(0)=="Activity dependencies validation failed for activity dependenciesCircular: Parallel transformation one-dlk contains circular dependencies: two-dlk")
      }
    }
  }

  test("ActivityDeltaTestSpec#Dependence not found"){
    val arguments = Array(
      "--adf-runid", "-99",
      "--transformation-activity", "dependenciesNotFound",
      "--config-file", TestingHelper.pathToTestConfigDelta)

    // Limpiamos estadísticas
    cleanStats()

    // Execution
    try {
      com.minsait.metagold.Main.main(arguments)
      assert(false) //debería fallar antes
    }
    catch {
      case e:Throwable=> {
        val statistics = spark.sql("select * from aplicaciones.transform order by datalake_load_date desc").select("resultMsg")
        assert(statistics.count() == 1)
        assert(statistics.collect()(0).getString(0) == "Activity dependencies validation failed for activity dependenciesNotFound: Parallel transformation one-dlk depends on non existing transformation six-dlk")
      }
    }
  }

  test("ActivityDeltaTestSpec#The transformation does not exist") {

    // Limpiamos estadísticas
    cleanStats()

    // Execution
    try {
      com.minsait.metagold.Main.main(argsTestWrongTransform)
      assert(false) //debe fallar antes
    }
    catch{
      case e:Throwable=>{
        // results assertion
        val statistics = spark.sql("select * from aplicaciones.transform order by datalake_load_date desc").select("resultMsg")
        assert(statistics.count()==1)
        assert(statistics.collect()(0).getString(0)=="Transformation transformation_that_doesnt_exist not defined")
      }
    }

  }

  test("ActivityDeltaTestSpec#The activity does not exist") {

    // Limpiamos estadísticas
    cleanStats()

    // Execution
    try {
      // Execution
      com.minsait.metagold.Main.main(argsTestWrongActivity)
      assert(false) //debe fallar antes
    }
    catch{
      case e:Throwable=> {
        // results assertion
        val statistics = spark.sql("select * from aplicaciones.transform order by datalake_load_date desc limit 1").select("resultMsg")
        assert(statistics.count() == 1)
        assert(statistics.collect()(0).getString(0) == "Activity not defined: activityNotExists")
      }
    }
  }

  test("ActivityDeltaTestSpec#First activity Ok, Second activity Fails, statistics should not fail") {

    // Execution
    com.minsait.metagold.Main.main(argsTestOkWrong1)
    com.minsait.metagold.Main.main(argsTestOkWrong2) // debe generar estadística de error, pero no debe fallar

    // results assertion
  }


  test("ActivityDeltaTestSpec#Full activity execution") {

    // Execution
    com.minsait.metagold.Main.main(args1)

    // Table results assertion
    val dataframe = readSQLTable(TestingHelper.SQLConnection, Some(s"$schemaDatamart.$tableAggProduct"), None, TestingHelper.indationPropertiesDelta)
    val filas = dataframe.collect()
    assert(dataframe.count==1)
    assert(dataframe.schema.length==2)
    assert(filas(0).getString(0)=="HL Road Frame - Black, 58")
    assert(filas(0)(1)==1)
  }

  test("ActivityDeltaTestSpec#Wrong activity execution") {

    // Limpiamos estadísticas
    cleanStats()

    // Execution
    try {
      // Execution
      val args = argsWrong
      com.minsait.metagold.Main.main(args)
      assert(false)
    } catch{
      case e:Throwable=> {
        // results assertion
        val statistics = spark.sql("select * from aplicaciones.transform order by datalake_load_date desc limit 1").select("resultMsg")
        assert(statistics.count() == 1)
        assert(statistics.collect()(0).getString(0) == "Activity not defined: wrongTestActivity")
      }
    }

  }

  test("ActivityDeltaTestSpec#Statistics Json Parser Test"){
    import com.minsait.metagold.activity.statistics.ActivityStatsJsonProtocol._
    import spray.json._

    val activityStage = ActivityTransformationStage(
      "stage1",
      "tipoStage",
      Success,
      1,
      ActivityDuration(new Timestamp(0),new Timestamp(1),1)
    )
    val activityTransform = ActivityTransformation(
      "transform1",
      "tipoTransform",
      Fail,
      ActivityDuration(new Timestamp(0),new Timestamp(1),1),
      List(activityStage),
      "mensaje",
      List("")
    )

    var activityStatistics =
      ActivityStatistics(
        "uuid",
        ActivityTrigger(Adf,"-111"),
        ActivityEngine(BuildInfo.name, BuildInfo.version),
        ActivityExecution("activity",List(ActivityParameter("param1","valor1"))),
        Success,
        ActivityDuration(new Timestamp(0),new Timestamp(1),1),
        List(activityTransform),
        "")
    assert(activityStatistics.toJson.toString().nonEmpty)
  }

  test("ActivityDeltaTestSpec#Statistics Writer Test"){

    // Execution
    com.minsait.metagold.Main.main(args2)

    // Read Statistics
    val dataframe = spark.table("aplicaciones.transform")

    assert(!dataframe.isEmpty)
  }

  test("getOutputType should return a OutputTypeException"){
    assertThrows[OutputTypeException] {
      DataHelper.getOutputType(DatalakeOutputTypes.DatalakeOutputType("avro"))

    }
  }

  //TODO: Añadir más tests

}
