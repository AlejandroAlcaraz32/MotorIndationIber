package com.minsait.common.configuration.models

case class SecurityEncryptionProperties(
                                         encryptionType: SecurityEncryptionTypes.SecurityEncryptionType = SecurityEncryptionTypes.None,
                                         masterKey: Option[String] = None,
                                         encryptRawFile: Option[Boolean] = Some(false),
                                         encryptionAlgorithm: Option[EncryptionAlgorithm] = None,
                                         hashSalt: Option[String] = None
                                       )