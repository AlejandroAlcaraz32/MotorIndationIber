package com.minsait.metagold.metadata.models

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models.IndationProperties

case class SQLConnection(
                    name: String,
                    description: String,
                    host: Option[String],
                    port: Option[String],
                    jdbcUrlKey: Option[String],
                    database: Option[String],
                    userKey: Option[String]=None,
                    passwordKey: Option[String]=None
                 ){

  //  def getJdbcUrl(metagoldProperties: MetagoldProperties):String = {  s"jdbc:sqlserver://$host:$port;database=$database"}

  /**
   * Returns Jdbc Connection String. If the jdbc url key is defined, it will be fetched from secret scope. In other case, it is formed by server data.
   * @return Jdbc url connection string
   */
  def getJdbcUrl(metagoldProperties: IndationProperties, forceSQLServer: Boolean=false):String={

    val url =
      if (jdbcUrlKey.isDefined){
//        // Modo URL almacenada en key vault (opción preferida para entornos productivos)
        metagoldProperties.getSecret(jdbcUrlKey.get)
      }
      else {
        // En cualquier otro caso, forma la url a partir de los datos de conexión
        if (metagoldProperties.environment==Local && !forceSQLServer) {
          // En modo local (Tests), cadena de conexión de H2
          s"jdbc:h2:~/${database.get};MODE=MSSQLServer;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_UPPER=FALSE;"
        }
        else{
          // En modo productivo, cadena de conexión estándar de SQL Server
          s"jdbc:sqlserver://${host.get}:${port.get};database=${database.get};encrypt=true;trustServerCertificate=true;"
        }
      }

    // devolvemos la URL resultante calculada
    url
  }
}
