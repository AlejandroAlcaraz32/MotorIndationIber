package com.minsait.common.spark

import com.minsait.common.configuration.models.{DatalakeOutputTypes, IndationProperties}
import com.minsait.common.configuration.models.EnvironmentTypes.Local
import com.minsait.common.configuration.models.EnvironmentTypes.Synapse
import com.minsait.indation.metadata.MetadataFilesManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object SparkSessionFactory {

	var spark: SparkSession = _
	var persistedDataframes: List[DataFrame] = List()
	var temporaryViews: List[String] = List()

	def getSparkSession: SparkSession = this.spark

	def stopSparkSession(): Unit = {
		// Clear the Spark catalog cache
		if (this.spark != null) {
			this.spark.catalog.clearCache()
		}

		// Stop the Spark session
		if (this.spark != null) {
			this.spark.stop()
			this.spark = null
		}

		// Clean the garbage collector
		System.gc()
	}

	def configSparkSession(indationProperties: IndationProperties, sparkSession: Option[SparkSession]=None): Unit = {
		if(this.spark != null) {
			return
		}

		if (indationProperties.environment == Local) {
			this.spark = SparkSession.builder()
				.appName("Indation")
				.master("local[*]")
				.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
				.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
				.config("spark.sql.shuffle.partitions", "1")
				.getOrCreate()

			this.spark.sparkContext.setLogLevel("ERROR")

		} else {
			this.spark = SparkSession.builder().getOrCreate().newSession()
			spark.conf.set("appName", "Indation")
			// spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
			spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
			//spark.conf.set("spark.hadoop.fs.permissions.umask-mode", "000")

			// Ampliación del tiempo de broadcast a 1 hora para evitar que falle cuando está redimensionando (originalmente está en 5 minutos)
			spark.conf.set("spark.sql.broadcastTimeout",  3600)
			spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100*1024*1024) // Recomendación encontrada en documentación

			// Optimizaciones para Delta Lake y Databricks
			spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", true)
			spark.conf.set("spark.databricks.delta.autoCompact.enabled", true)
			spark.conf.set("spark.databricks.io.cache.enabled", true)
			//spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)
			spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", true)

			spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
			spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
			spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
			spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

			// AQE
      		spark.conf.set("spark.sql.adaptive.enabled", true)
				if (indationProperties.environment == Synapse) {
				// Configurar permisos de acceso a datalake
				configAzureStorageAccess(
					indationProperties, indationProperties.datalake.storageAccountName,
					indationProperties.datalake.storageAccountAccessKey)

				// Configurar permisos de acceso a landing
				if(!indationProperties.datalake.storageAccountName.equals(indationProperties.landing.storageAccountName)) {
				configAzureStorageAccess(
					indationProperties, indationProperties.landing.storageAccountName,
					indationProperties.landing.storageAccountAccessKey)
				}

				// Configurar permisos de acceso a metadata
				if(!indationProperties.datalake.storageAccountName.equals(indationProperties.metadata.storageAccountName)) {
				configAzureStorageAccess(
					indationProperties, indationProperties.metadata.storageAccountName,
					indationProperties.metadata.storageAccountAccessKey)
				}
			}
		}

		// Comportamiento de sobrescritura únicamente en particiones en formato parquet
		if (indationProperties.datalake.outputType.isDefined &&
			indationProperties.datalake.outputType.get==DatalakeOutputTypes.Parquet)
			spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

		// para no tener fallos con el parseado de fechas en spark > 3.0.0
		spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
		spark.conf.set("spark.sql.legacy.createHiveTableByDefault", false)
		spark.conf.set("spark.sql.debug.maxToStringFields", 4096)

		// realizamos una comprobación de acceso
		var success = false
		var attempts = 3
		while (!success && attempts>3){
			attempts -= 1
			try{
				val metadataManager = new MetadataFilesManager(indationProperties)
				val datasets = metadataManager.datasets
				success = true
			} catch {
				case e:Throwable=>{
					if (attempts==0){
						throw new SecurityException(s"Error validating file permission after spark configuration.",e)
					}
					else{
						println(s"Error validating file permission. Pending attempts after spark configuration: $attempts")
						Thread.sleep(1000)
					}
				}
			}

		}

	}

	private def configAzureStorageAccess(indationProperties: IndationProperties, storageAccountName: String, accountAccessKey: Option[String]): Unit = {

		if (indationProperties.security.identity.useADLSAccountKey.getOrElse(false)){
			// en caso de que se haya especificado seguridad por clave de cuenta, será necesario establecer la seguridad de este modo (método inseguro que no aplica ACL, da acceso completo)
			if (accountAccessKey.isDefined){
				val accessKey = indationProperties.getSecret(accountAccessKey.get)
				spark.conf.set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", accessKey)
				spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", accessKey)
			}
			else{
				throw new SecurityException(s"ADLS access account key mode has ben defined in security options, but no access key has been provided in configuration.")
			}

		}
		else {
			// seguridad de acceso estándar por service principal
			val servicePrincipalAppId = indationProperties.getSecret(indationProperties.security.identity.servicePrincipalAppIdKey)
			val servicePrincipalTenantId = indationProperties.getSecret(indationProperties.security.identity.servicePrincipalTenantIdKey)
			val servicePrincipalPassword = indationProperties.getSecret(indationProperties.security.identity.servicePrincipalPasswordKey)

			spark.conf.set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
			spark.conf.set("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
			spark.conf.set("fs.azure.account.oauth2.client.id." + storageAccountName + ".dfs.core.windows.net", servicePrincipalAppId)
			spark.conf.set("fs.azure.account.oauth2.client.secret." + storageAccountName + ".dfs.core.windows.net", servicePrincipalPassword)
			spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + servicePrincipalTenantId + "/oauth2/token")

			// spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
			// spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
			// spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id." + storageAccountName + ".dfs.core.windows.net", servicePrincipalAppId)
			// spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret." + storageAccountName + ".dfs.core.windows.net", servicePrincipalPassword)
			// spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint." + storageAccountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + servicePrincipalTenantId + "/oauth2/token")
		}
	}

}
