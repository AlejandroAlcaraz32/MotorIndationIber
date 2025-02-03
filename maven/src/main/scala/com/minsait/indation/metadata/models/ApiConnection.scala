package com.minsait.indation.metadata.models

import com.minsait.common.configuration.models.IndationProperties
import com.minsait.indation.metadata.models.enums.ApiAuthenticationTypes.ApiAuthenticationType

case class ApiConnection(
                          authenticationType: ApiAuthenticationType,
                          url: String,
                          userKey: Option[String],
                          passwordKey: Option[String]
                         ){

  def getUser(indationProperties: IndationProperties):String={

    indationProperties.getSecret(userKey.get )

  }

  def getPassword(indationProperties: IndationProperties):String={

    indationProperties.getSecret(passwordKey.get )

  }

}
