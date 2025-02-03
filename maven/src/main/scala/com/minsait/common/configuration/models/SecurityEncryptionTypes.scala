package com.minsait.common.configuration.models

object SecurityEncryptionTypes {
	sealed case class SecurityEncryptionType(value: String)
	object Pseudonymization extends SecurityEncryptionType("pseudonymization")
	object None extends SecurityEncryptionType("none")
}
