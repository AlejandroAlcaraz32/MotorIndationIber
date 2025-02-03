package com.minsait.common.configuration.models

object EncryptionAlgorithmTypes {
	sealed case class EncryptionAlgorithmType(value: String)
	object Aes extends EncryptionAlgorithmType("AES")
}
