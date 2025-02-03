package com.minsait.common.configuration

import com.minsait.common.configuration.exceptions.{ConfigurationException, ConfigurationTypeException}
import com.minsait.common.configuration.exceptions.ConfigurationTypeException
import com.minsait.common.configuration.models.EnvironmentTypes.EnvironmentType
import com.minsait.common.configuration.models.{DatalakeProperties, EnvironmentTypes, IndationProperties, 
	LandingProperties, MetadataDatasourceProperties}
import com.minsait.common.configuration.models._
import zio._
import zio.config._
import zio.config.magnolia._
import zio.ConfigProvider
import zio.Config, Config._
import zio.config.yaml._

import java.io.{FileInputStream, FileNotFoundException, InputStreamReader}
import scala.collection.JavaConverters._

/**
 * Class responsible for read properties file.
 */
class ConfigurationManager(val propertiesFilePath: String) {

	val safeIndationProperties = ConfigReader.readConfig(propertiesFilePath)
  	val indationProperties: IndationProperties = Unsafe.unsafe(
    	implicit u => zio.Runtime.default.unsafe.run(safeIndationProperties)
      .getOrThrow()
  	)
	

	def metadataDatasource: MetadataDatasourceProperties = {
		val mdp = indationProperties.metadata.datasource.getOrElse(null)
		MetadataDatasourceProperties(mdp.server, mdp.port, mdp.database, mdp.username,
			getSecretsScope, mdp.passwordkey, mdp.driver, mdp.protocol, mdp.options)
  	}

	private def getSecretsScope: Option[String] = for {
			properties <- indationProperties.databricks
	} yield properties.secrets.scope

	def getDatalakeProperties: DatalakeProperties = indationProperties.datalake

	def getLandingProperties: LandingProperties = indationProperties.landing

	def getEnvironment: EnvironmentType = {
		val env = indationProperties.environment.value
		try{
			EnvironmentTypes.EnvironmentType(env)
		} catch {
			case ex: NoSuchElementException =>
				throw new ConfigurationTypeException("Enviroment "+ env +" not valid", ex.getCause)
		}
	}
	private object ConfigReader {
		def readConfig(configPath: String): IO[Throwable, IndationProperties] = {
			val configString = ZIO.attempt(scala.io.Source.fromFile(configPath).mkString)
			val configProvider: IO[Throwable, ConfigProvider] = for {
				configString <- configString
			} yield { ConfigProvider.fromYamlString(configString) }
			configProvider.foldZIO(
				failure => ZIO.fail(failure),
				success => success.load(deriveConfig[IndationProperties]))
		}
	}
}