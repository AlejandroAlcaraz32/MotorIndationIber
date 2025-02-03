package com.minsait.common.configuration.models

object EncryptionModeTypes {
	sealed case class EncryptionModeType(value: String)
	object Ctr extends EncryptionModeType("CTR")
	object Ofb extends EncryptionModeType("OFB")
	object Cfb extends EncryptionModeType("CFB")
}
