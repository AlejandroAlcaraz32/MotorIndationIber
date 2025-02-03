package com.minsait.common.configuration.models

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.microsoft.aad.adal4j.{AuthenticationContext, ClientCredential}

import java.util.concurrent.Executors
import com.microsoft.azure.synapse.tokenlibrary.TokenLibrary

case class IndationProperties(
                               metadata: MetadataProperties,
                               databricks: Option[DatabricksProperties],
                               landing: LandingProperties,
                               datalake: DatalakeProperties,
                               security: SecurityProperties,
                               environment: EnvironmentTypes.EnvironmentType,
                               tmpDirectory: String,
                               graylog: Option[GraylogProperties]=None,
                               synapse: Option[SynapseProperties]=None,
                               silverStatistics: Option[StatisticsProperties]=None,
                               goldStatistics: Option[StatisticsPropertiesGold]=None
                             ){

  def getSecret(secretKey: String): String={
    environment match {
      case EnvironmentTypes.Databricks => {

        dbutils.secrets.get(
          scope = databricks.get.secrets.scope,
          key = secretKey
        )

      }

      case EnvironmentTypes.Synapse =>{

        TokenLibrary.getSecret(
          akvName = synapse.get.secrets.keyVault,
          secret = secretKey,
          linkedService = synapse.get.secrets.linkedService
        )

      }
      case EnvironmentTypes.ManagedIdentity =>{
        // En MANAGED IDENTITY devolvemos el valor propio de la clave
        secretKey
      }
      case EnvironmentTypes.Local =>{
        // En entorno local devolvemos el valor propio de la clave
        secretKey
      }
      case _ =>
        throw new NoSuchElementException(s"Environment ${environment.value} not valid.")
    }
  }

  def getAcessToken(): String={

    val appId = getSecret(security.identity.servicePrincipalAppIdKey)
    val secret = getSecret(security.identity.servicePrincipalPasswordKey)
    val tenantId = getSecret(security.identity.servicePrincipalTenantIdKey)

    // conexión con ADAL
    val authority = "https://login.windows.net/" + tenantId
    val resourceAppIdURI = "https://database.windows.net/"
    val ServicePrincipalId = appId
    val ServicePrincipalPwd = secret

    //Instantiate the ADAL AuthenticationContext object
    val service = Executors.newFixedThreadPool(1)
    val context = new AuthenticationContext(authority, true, service);

    //Get access token
    val ClientCred = new ClientCredential(ServicePrincipalId, ServicePrincipalPwd)
    val authResult = context.acquireToken(resourceAppIdURI, ClientCred, null)
    authResult.get().getAccessToken
  }
}
