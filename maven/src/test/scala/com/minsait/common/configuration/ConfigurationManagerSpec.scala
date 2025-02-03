package com.minsait.common.configuration

import com.minsait.common.configuration.exceptions.ConfigurationException
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models._
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import java.io.FileNotFoundException

class ConfigurationManagerSpec extends AnyFunSuite with MockitoSugar {

	test("ConfigurationManager#getProperties when path is correct and properties are not defined yet should not thrown an error.") {
		// Given
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		new ConfigurationManager(propertiesFilePath)

		succeed
	}

	test("ConfigurationManager#getProperties when yml does not contain graylog configuration should not thrown an error.") {
		// Given
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		val config = new ConfigurationManager(propertiesFilePath)
		assert(config.indationProperties.graylog.isEmpty)
	}

	test("ConfigurationManager#getProperties when graylog is defined should create graylog config class in configuration.") {
		// Given
		val propertiesFilePath = this.getClass.getResource("/indation-graylog-config.yml").getPath
		val config = new ConfigurationManager(propertiesFilePath)
		assert(config.indationProperties.graylog.isDefined)
	}

	test("ConfigurationManager#getProperties when path is wrong and properties are not defined yet should throw a ConfigurationException.") {
		// Given
		val propertiesFilePath = "/wrong-path.yml"

		// When - Then
		assertThrows[FileNotFoundException] {
			new ConfigurationManager(propertiesFilePath)
		}
	}

	test("ConfigurationManager#getMetadataSource should return expected IndationMetadataSource.") {
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		val configManager = new ConfigurationManager(propertiesFilePath)
		// Given

		val server = "server1"
		val port = "5432"
		val database = "indation_metadata"
		val username = "user1"
		val passwordkey = "passkey1"
		val secretScope = Some("scope1")
		val driver = "org.postgresql.Driver"
		val protocol = "jdbc:postgresql"
		val options = Map("sslmode" -> "allow")

		val expectedMetadataSource = MetadataDatasourceProperties(server, port, database, username, secretScope, passwordkey, driver, protocol, options)

		// When
		val actualMetadataSource = configManager.metadataDatasource

		// Then
		assert(actualMetadataSource == expectedMetadataSource)
	}

	test("ConfigurationManager#getDatalakeProperties should return expected base path.") {
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		val configManager = new ConfigurationManager(propertiesFilePath)
		// Given
		val storageAccountName = "storageAccount1"
		val basePath = "/DatalakeTestPath"
		val container = "DatalakeContainterTest"
		val expectedDatalakeProperties = DatalakeProperties(storageAccountName, basePath, container)

		// When
		val actualDatalakeProperties = configManager.indationProperties.datalake

		// Then
		assert(actualDatalakeProperties == expectedDatalakeProperties)
	}

	test("ConfigurationManager#getEnvironment should return expected environment.") {
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		val configManager = new ConfigurationManager(propertiesFilePath)
		// Given
		val expectedEnv = Local

		// When
		val actualEnv = configManager.indationProperties.environment

		// Then
		assert(actualEnv == expectedEnv)
	}

	test("ConfigurationManager#getLandingProperties should return expected landing properties.") {
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		val configManager = new ConfigurationManager(propertiesFilePath)
		// Given
		val storageAccountName = "LandingStorageAccount"
		val basePath = "/LandingTestPath"
		val container = "LandingContainerTest"
		val pendingDirectory = "LandingPendingDirectory"
		val unknownDirectory = "LandingUnknownDirectory"
		val invalidDirectory = "LandingInvalidDirectory"
		val corruptedDirectory = "LandingCorruptedDirectory"
		val schemaMismatchDirectory = "LandingSchemaMismatchDirectory"
		val streamingDirectory = "LandingStreamingDirectory"

		val expectedLandingProperties = LandingProperties(storageAccountName, basePath, container
			, pendingDirectory, unknownDirectory, invalidDirectory, corruptedDirectory, schemaMismatchDirectory, streamingDirectory)

		// When
		val actualLandingProperties = configManager.indationProperties.landing

		// Then
		assert(actualLandingProperties == expectedLandingProperties)
	}

	test("ConfigurationManager#DatabricksProperties should return expected Databricks properties.") {
		val propertiesFilePath = this.getClass.getResource("/indation-test-config.yml").getPath
		val configManager = new ConfigurationManager(propertiesFilePath)
		// Given
		val scope = "scope1"

		val expectedDatabricksProperties = DatabricksProperties(DatabricksSecretsProperties(scope))

		// When
		val databricksProperties = configManager.indationProperties.databricks.get

		// Then
		assert(databricksProperties == expectedDatabricksProperties)
	}
}
