package com.minsait.indation.metadata.models

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.indation.metadata.models.enums.JdbcAuthenticationTypes.JdbcAuthenticationType
import com.minsait.indation.metadata.models.enums.JdbcDriverTypes._


case class JdbcConnection(
                           driver: JdbcDriverType,
                           host: Option[String],
                           port: Option[String],
                           database: Option[String],
                           sid: Option[String],
                           jdbcUrlKey: Option[String],
                           authenticationType: JdbcAuthenticationType,
                           userKey: Option[String],
                           passwordKey: Option[String]
                         ){

  def getUser(indationProperties: IndationProperties):String={

    indationProperties.getSecret(userKey.get )

  }

  def getPassword(indationProperties: IndationProperties):String={

    indationProperties.getSecret(passwordKey.get )

  }

  /**
   * Returns Jdbc Connection String. If the jdbc url key is defined, it will be fetched from secret scope. In other case, it is formed by server data.
   * @return Jdbc url connection string
   */
  def getConnectionUrl(indationProperties: IndationProperties):String={

    if (jdbcUrlKey.isDefined){
      indationProperties.getSecret(jdbcUrlKey.get )
    }
    else {
      // En cualquier otro caso, forma la url a partir de los datos de conexión
      driver match {
        case Oracle => s"jdbc:oracle:thin:@${host.get}:${port.get}:${sid.get}"
        case MSSql => s"jdbc:sqlserver://${host.get}:${port.get};database=${database.get};encrypt=true;trustServerCertificate=true;" //;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
        case H2 =>    s"jdbc:h2:~/${database.get};MODE=MSSQLServer;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_UPPER=FALSE;" // Sólo en modo fichero y únicamente para los tests
        case MySQL =>    s"jdbc:mysql://${host.get}:${port.get}/${database.get}"
        case SAP =>    s"jdbc:sap://${host.get}:${port.get}/${database.get}"
        case _ => throw new UnsupportedOperationException("Unsupported Jdbc Table Input Driver " + driver)
      }
    }
  }


}
