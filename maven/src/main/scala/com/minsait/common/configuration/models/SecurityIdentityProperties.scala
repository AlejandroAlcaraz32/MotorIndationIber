package com.minsait.common.configuration.models

case class SecurityIdentityProperties(
                                       servicePrincipalAppIdKey: String,
                                       servicePrincipalTenantIdKey: String,
                                       servicePrincipalPasswordKey:String,
                                       useADLSAccountKey: Option[Boolean]=Some(false)
                                     )