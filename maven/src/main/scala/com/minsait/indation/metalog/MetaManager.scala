package com.minsait.indation.metalog

import com.minsait.common.configuration.ConfigurationManager
import com.minsait.common.configuration.models.MetadataDatasourceProperties
import com.minsait.indation.metalog.exceptions.DatabaseException
import org.postgresql.util.PSQLException

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * Class responsible for manage the connection to the PostgreSQL database.
 */
class MetaManager {

	/* Object to read YAML properties file. */
	protected var configManager: ConfigurationManager = _

	/* Object to connect to the metadata database. */
	protected var dbConn: Connection = _

	/**
	 * Method to initialize the connection to the PostgreSQL database. It will be called on the primary constructor.
	 * @return A configured Connection object.
	 */
	protected def initMetadataDbConnection: Connection = {

		val metadataDatasource = configManager.metadataDatasource

		try {

			var dbPassword : String = ""

//			if(configManager.getEnvironment == EnvironmentTypes.Local) {
//				 dbPassword = metadataDatasource.passwordkey
//			}
//			else {
//				//TODO: Validar si esto es correcto, ya que puede que metadataDatasource esté configurado para tener un scope propio
//				dbPassword = configManager.indationProperties.getSecret(metadataDatasource.passwordkey)
//			}
			dbPassword = configManager.indationProperties.getSecret(metadataDatasource.passwordkey)

			Class.forName(metadataDatasource.driver)

			val connProps = new Properties()
			connProps.setProperty("user", metadataDatasource.username)
			connProps.setProperty("password", dbPassword)
			metadataDatasource.options.foreach( option => connProps.setProperty(option._1, option._2))

			val dbConn = DriverManager.getConnection(this.url(metadataDatasource), connProps)
			dbConn
		}
		catch {
			case ex: PSQLException => throw new DatabaseException(ex.getMessage, ex.getCause)
		}
	}

	private def url(metadataDatasource: MetadataDatasourceProperties): String = {
		var url = new StringBuilder(metadataDatasource.protocol)
		if(!metadataDatasource.server.isEmpty) url = url.append("://" + metadataDatasource.server)
		if(!metadataDatasource.port.isEmpty) url = url.append(":" + metadataDatasource.port)
		if(!metadataDatasource.database.isEmpty) url = url.append("/"+ metadataDatasource.database)

		url.toString()
	}
}
