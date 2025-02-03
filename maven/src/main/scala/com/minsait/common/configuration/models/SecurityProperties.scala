package com.minsait.common.configuration.models

case class SecurityProperties(
                               identity: SecurityIdentityProperties,
                               encryption: SecurityEncryptionProperties
                             )
