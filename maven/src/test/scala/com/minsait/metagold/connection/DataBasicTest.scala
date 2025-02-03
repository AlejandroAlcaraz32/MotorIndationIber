package com.minsait.metagold.connection

import com.minsait.common.configuration.models.EnvironmentTypes.Databricks
import com.minsait.common.spark.SparkSessionFactory
import com.minsait.common.utils.DataHelper
import com.minsait.common.utils.DataHelper.getJdbcBaseConnectionProperties
import com.minsait.metagold.helpers.TestingHelper
import com.minsait.metagold.helpers.TestingHelper.assertDataframeEquals
import com.minsait.metagold.metadata.models.SQLConnection
import com.minsait.metagold.metadata.models.enums.WriteModes
import org.apache.spark.sql.functions.{col, lit}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite


class DataBasicTestSpec extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll  {

  override def beforeAll() {
    TestingHelper.InitSparkSessionDelta
    TestingHelper.PrepareTestData(TestingHelper.indationPropertiesDelta)
  }

  test("load silver table"){
    val dataframe = DataHelper.readSilverTable(
      TestingHelper.indationPropertiesDelta,
      TestingHelper.silverProductTable
    )
    assert(dataframe.count()==606)
  }

  test("load gold table"){
    val dataframe = DataHelper.readGoldTable(
      TestingHelper.indationPropertiesDelta,
      TestingHelper.dlkProductFolder
    )
    assert(dataframe.count()==606)
  }

  test("write sql table overwrite"){
    val tableName = s"${TestingHelper.schemaStaging}.${TestingHelper.tableProductOverwrite}"

    val dfSilver = TestingHelper.getDfSilverProduct(TestingHelper.indationPropertiesDelta)

    DataHelper.writeSQLTable(
      TestingHelper.SQLConnection,
      tableName,
      WriteModes.OverwriteMode,
      dfSilver,
      TestingHelper.indationPropertiesDelta
    )

    val dfGold = DataHelper.readSQLTable(
      TestingHelper.SQLConnection,
      Some(tableName),
      None,
      TestingHelper.indationPropertiesDelta
    )

    assertDataframeEquals(dfSilver,dfGold)

  }

  test("write sql table append"){
    val tableName = s"${TestingHelper.schemaStaging}.${TestingHelper.tableProductAppend}"

    val dfSilver = TestingHelper.getDfSilverProduct(TestingHelper.indationPropertiesDelta).cache()

    // cargamos una fila
    DataHelper.writeSQLTable(
      TestingHelper.SQLConnection,
      tableName,
      WriteModes.AppendMode,
      dfSilver.where(col("ProductKey")===lit(1)),
      TestingHelper.indationPropertiesDelta
    )

    // cargamos las demás filas
    DataHelper.writeSQLTable(
      TestingHelper.SQLConnection,
      tableName,
      WriteModes.AppendMode,
      dfSilver.where(col("ProductKey")>lit(1)),
      TestingHelper.indationPropertiesDelta
    )

    // el resultado debe ser que sean iguales
    val dfGold = DataHelper.readSQLTable(
      TestingHelper.SQLConnection,
      Some(tableName),
      None,
      TestingHelper.indationPropertiesDelta
    )

    assertDataframeEquals(dfSilver,dfGold)

  }

  test("write sql table truncate"){
    val tableName = s"${TestingHelper.schemaStaging}.${TestingHelper.tableProductTruncate}"

    val dfSilver = TestingHelper.getDfSilverProduct(TestingHelper.indationPropertiesDelta).cache()

    // cargamos una fila
    DataHelper.writeSQLTable(
      TestingHelper.SQLConnection,
      tableName,
      WriteModes.TruncateMode,
      dfSilver.where(col("ProductKey")===lit(1)),
      TestingHelper.indationPropertiesDelta
    )

    // cargamos las demás filas
    DataHelper.writeSQLTable(
      TestingHelper.SQLConnection,
      tableName,
      WriteModes.TruncateMode,
      dfSilver.where(col("ProductKey")>lit(1)),
      TestingHelper.indationPropertiesDelta
    )

    // el resultado debe ser que en gold falte el primer registro
    val dfGold = DataHelper.readSQLTable(
      TestingHelper.SQLConnection,
      Some(tableName),
      None,
      TestingHelper.indationPropertiesDelta
    )

    assertDataframeEquals(
      dfSilver.where(col("ProductKey")>lit(1)),
      dfGold
    )

  }

//  test("read synapse sql endpoint"){
//
//    // datos de conexión
//    val sqlConnection = SQLConnection(
//      name = "connTest",
//      description = "connTests",
//      host = Some("hei-aep-es001-d-we-syn-01.sql.azuresynapse.net"),
//      port = Some("1433"),
//      jdbcUrlKey = None,
//      database = Some("datamodel"),
//      userKey = Some("sqlAdmin"),
//      passwordKey = Some("Rq[gLK[-n?XEa;!AwKPd")
//    )
//
//    // generamos url en formato sql server (indicamos que no estamos en modo test para que no intente H2)
//    var url = sqlConnection.getJdbcUrl(TestingHelper.indationPropertiesDelta, true)
//
//    // obtenemos las propiedades de conexión (indicamos que no estamos en modo test para que no intente H2)
//    val connectionProperties = getJdbcBaseConnectionProperties(TestingHelper.indationPropertiesDelta, sqlConnection, true)
//
//    // leemos la tabla
//    val df = SparkSessionFactory.spark.read.jdbc(url, "INFORMATION_SCHEMA.TABLES", connectionProperties)
//
//    assert(!df.isEmpty)
//  }


}
