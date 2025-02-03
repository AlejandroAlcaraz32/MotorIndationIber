package com.minsait.metagold.metadata

import com.minsait.common.spark.SparkSessionWrapper

import com.minsait.metagold.helpers.TestingHelper
import com.minsait.metagold.metadata.models.{
  Activity, AggregationColumn, DatalakeTransformation,
  DatalakeTransformationStage, GoldTableSource, JoinExpr, Parameter, SQLConnection, SQLTransformation,
  SQLTransformationStage, StageAggregation, StageBinarizer, StageBucketizer, StageColumn, StageFiller, StageFilter,
  StageImputer, StageOneHotEncoder, StageSelect, StageTable, StageVariation, Transformation, TransformationExecution
}
import com.minsait.metagold.metadata.exceptions.NonUniqueDatasetException
import com.minsait.metagold.metadata.models.enums.ActivityTypes.ActivityType
import com.minsait.metagold.metadata.models.enums.AggregationTypes.AggregationType
import com.minsait.metagold.metadata.models.enums.CalculationTypes.CalculationType
import com.minsait.metagold.metadata.models.enums.{ColsTypes, FillerInputTypes}
import com.minsait.metagold.metadata.models.enums.FilterTypes.FilterType
import com.minsait.metagold.metadata.models.enums.JoinTypes.JoinType
import com.minsait.metagold.metadata.models.enums.StageTypes.StageType
import com.minsait.metagold.metadata.models.enums.TableTypes.TableType
import com.minsait.metagold.metadata.models.enums.TransformationTypes.TransformationType
import com.minsait.metagold.metadata.models.enums.WriteModes.WriteMode
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import matchers.should._

class MetadataFilesManagerTestSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll with Matchers {

  var metadataManager: MetadataFilesManager = _

  override def beforeAll() {
    TestingHelper.InitSparkSessionDelta
    metadataManager = new MetadataFilesManager(TestingHelper.indationPropertiesParquet)
  }

  test("Get Connections") {
    val connections = metadataManager.sqlConnections
    assert(connections.nonEmpty)
    assert(connections.length == 2)
  }

  test("Get Activities") {
    val activities = metadataManager.activities
    assert(activities.nonEmpty)
    assert(activities.length == 17)
  }

  test("Get Transformations") {
    val transformations = metadataManager.transformations
    assert(transformations.nonEmpty)
    assert(transformations.length == 21)
  }

  test("Get Connection By Name") {
    val connection = metadataManager.sqlConnectionByName("testConnection")
    assert(connection.nonEmpty)
  }

  test("Get Connection By Name should return NonUniqueDatasetException") {

    assertThrows[NonUniqueDatasetException] {
      metadataManager.sqlConnectionByName("duplicado")
    }
  }

  test("Get Activity By Name") {
    val activities = metadataManager.activityByName("testActivity")
    assert(activities.nonEmpty)
  }

  test("Get Transformation By Name") {
    val transformationDlk = metadataManager.transformationByName("product-subcategory-dlk")
    val transformationSql = metadataManager.transformationByName("plain-product-sql")
    assert(transformationDlk.nonEmpty)
    assert(transformationDlk.get.datalakeTransformation.isDefined)
    assert(transformationSql.nonEmpty)
    assert(transformationSql.get.sqlTransformation.isDefined)
  }

  test("Get Transformation By Name demo-transformacion") {
    val transformationDlk = metadataManager.transformationByName("demo-transformacion-dlk")
    val transformationSql = metadataManager.transformationByName("demo-transformacion-sql")
    assert(transformationDlk.nonEmpty)
    assert(transformationDlk.get.datalakeTransformation.isDefined)
    assert(transformationSql.nonEmpty)
    assert(transformationSql.get.sqlTransformation.isDefined)
  }

  test("Conection Validator OK") {
    val path = "src/test/resources/metagold/metadataError"
    val bad_conection_files = metadataManager.validateMetadata(path)
    assert(bad_conection_files._1.length == 4)

  }

  test("Transformation Validator OK") {
    val path = "src/test/resources/metagold/metadataError"
    val bad_transformation_files = metadataManager.validateMetadata(path)

    assert(bad_transformation_files._2.length == 2)

  }

  test("Activity Validator OK") {
    val path = "src/test/resources/metagold/metadataError"

    val bad_activities_files = metadataManager.validateMetadata(path)

    assert(bad_activities_files._3.length == 2)

  }

  test("Main Activity returns java.lang.RuntimeException") {
    val arguments = Array(
      "--adf-runid", "-99",
      "--validate-activities", TestingHelper.pathMetadataerror,
      "--config-file", TestingHelper.pathToTestConfigDelta)

    assertThrows[java.lang.RuntimeException] {
      com.minsait.metagold.Main.main(arguments)
    }
  }

  test("Main Transformation Validator returns java.lang.RuntimeException") {
    val arguments = Array(
      "--adf-runid", "-99",
      "--validate-transformations", TestingHelper.pathMetadataerror,
      "--config-file", TestingHelper.pathToTestConfigDelta)

    assertThrows[java.lang.RuntimeException] {
      com.minsait.metagold.Main.main(arguments)
    }
  }

  test("Main SQLConnection returns java.lang.RuntimeException") {
    val arguments = Array(
      "--adf-runid", "-99",
      "--validate-connections", TestingHelper.pathMetadataerror,
      "--config-file", TestingHelper.pathToTestConfigDelta)

    assertThrows[java.lang.RuntimeException] {
      com.minsait.metagold.Main.main(arguments)
    }
  }

  test("Creation of metadata models instances should be of their respective classes") {

    // Instanciar diferentes clases
    val stagefilter = StageFilter(FilterType("expr"), Some("col(\"col1\"==1"))
    val stageactivity = Activity("Activity1", "Description", Some(ActivityType("concurrent")), None, None, None)
    val aggregationcol = AggregationColumn("agg1", "Description", AggregationType("max"), "col1")
    val datalaketransformation = DatalakeTransformation("classification", "database", "table", None, WriteMode("append"),
      None, List(),None)
    val datalaketransformationstage = DatalakeTransformationStage("name", "description", StageType("aggregation"), None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    val goldtableSource = GoldTableSource(None, None, None, None, None, "sqlConnection", "sqlSchema", "sqlTable",
      WriteMode("append"), true, true, None)
    val joinexpr = JoinExpr(JoinType("inner"), "expresion")
    val parameter = Parameter("shortName", "longName", "description")
    val sqlconnection = SQLConnection("name", "description", None, None, None, None)
    val sqltransformation = SQLTransformation(List(), List())
    val stageselect = StageSelect("columnName", "description", "columnAlias")
    val stageAggregation = StageAggregation("name", "string", List(), List())
    val stagebucketizer = StageBucketizer("name", "descripción", ColsTypes.ColsTypes("single"), None, None, None, None,
      None, None)
    val sqltransformationstage = SQLTransformationStage("connection", "destschema", "destable", "expre", None,
      WriteMode("append"), true, true, None)
    val stagebinarizer = StageBinarizer("name", "description", ColsTypes.ColsTypes("single"), Some(1), None, None, None,
      None, None)
    val stagecolumn = StageColumn("name", "description", CalculationType("expr"), None)
    val stagefiller = StageFiller("name", "description", FillerInputTypes.FillerInputTypes("string"), "value", None)
    val stagetable = StageTable("name", TableType("silver"), None, "table", None, None, None)
    val stageVariation = StageVariation("name", "description", true, 8, "timeorder", "inputcol", "outputcol")
    val transformation = Transformation("name", "description", TransformationType("datalake"), None, None)
    val transformationexecution = TransformationExecution("name", List())
    val stageonehotenconder = StageOneHotEncoder("name", "description", ColsTypes.ColsTypes("single"), stagetable,
      None, None, None, None)
    val stageimputer = StageImputer("name", "description", ColsTypes.ColsTypes("single"), None, None, None, None, None,
      None)

    //Comprobar que cada hace tiene los valores correspondientes a su instanciacion
    StageFilter.unapply(stagefilter).get shouldBe(FilterType("expr"), Some("col(\"col1\"==1"))
    Activity.unapply(stageactivity).get shouldBe("Activity1", "Description", Some(ActivityType("concurrent")), None,
      None, None)
    AggregationColumn.unapply(aggregationcol).get shouldBe("agg1", "Description", AggregationType("max"), "col1")
    DatalakeTransformation.unapply(datalaketransformation).get shouldBe("classification", "database", "table", None,
      WriteMode("append"), None, List(),None)
    DatalakeTransformationStage.unapply(datalaketransformationstage).get shouldBe("name", "description",
      StageType("aggregation"), None,
      None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
    GoldTableSource.unapply(goldtableSource).get shouldBe(None, None, None, None, None, "sqlConnection", "sqlSchema",
      "sqlTable", WriteMode("append"), true, true, None)
    JoinExpr.unapply(joinexpr).get shouldBe(JoinType("inner"), "expresion")
    Parameter.unapply(parameter).get shouldBe("shortName", "longName", "description")
    SQLConnection.unapply(sqlconnection).get shouldBe("name", "description", None, None, None, None, None, None)
    SQLTransformation.unapply(sqltransformation).get shouldBe(List(), List())
    StageSelect.unapply(stageselect).get shouldBe("columnName", "description", "columnAlias")
    StageAggregation.unapply(stageAggregation).get shouldBe("name", "string", List(), List())
    StageBucketizer.unapply(stagebucketizer).get shouldBe("name", "descripción", ColsTypes.ColsTypes("single"), None,
      None, None, None, None, None)
    SQLTransformationStage.unapply(sqltransformationstage).get shouldBe("connection", "destschema", "destable", "expre",
      None, WriteMode("append"), true, true, None)
    StageBinarizer.unapply(stagebinarizer).get shouldBe("name", "description", ColsTypes.ColsTypes("single"), Some(1),
      None, None, None, None, None)
    StageColumn.unapply(stagecolumn).get shouldBe("name", "description", CalculationType("expr"), None)
    StageFiller.unapply(stagefiller).get shouldBe("name", "description", FillerInputTypes.FillerInputTypes("string"),
      "value", None)
    StageTable.unapply(stagetable).get shouldBe("name", TableType("silver"), None, "table", None, None, None)
    StageVariation.unapply(stageVariation).get shouldBe("name", "description", true, 8, "timeorder", "inputcol",
      "outputcol")
    Transformation.unapply(transformation).get shouldBe("name", "description", TransformationType("datalake"), None,
      None)
    TransformationExecution.unapply(transformationexecution).get shouldBe("name", List())
    StageOneHotEncoder.unapply(stageonehotenconder).get shouldBe("name", "description", ColsTypes.ColsTypes("single"),
      stagetable, None, None, None, None)
    StageImputer.unapply(stageimputer).get shouldBe("name", "description", ColsTypes.ColsTypes("single"), None, None, None,
      None, None, None)


  }


  //  test("Get Transformation By Name Metadata2"){
  //    val transformationDlk = metadataManager.transformationByName("dim_tipo_serv-dlk")
  //    assert(transformationDlk.nonEmpty)
  //    assert(transformationDlk.get.datalakeTransformation.isDefined)
  //
  //  }

  //TODO: Añadir más tests

}
