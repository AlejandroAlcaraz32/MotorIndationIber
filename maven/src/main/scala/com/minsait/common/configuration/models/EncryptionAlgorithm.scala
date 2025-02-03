package com.minsait.common.configuration.models

case class EncryptionAlgorithm(
                                algorithm: EncryptionAlgorithmTypes.EncryptionAlgorithmType,
                                mode: EncryptionModeTypes.EncryptionModeType,
                                padding: String
                              )