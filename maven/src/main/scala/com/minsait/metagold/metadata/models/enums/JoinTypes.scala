package com.minsait.metagold.metadata.models.enums

object JoinTypes {
	sealed case class JoinType(value: String)
	object inner extends JoinType("inner")
	object cross extends JoinType("cross")
	object fullouter extends JoinType("fullouter")
	object leftanti extends JoinType("leftanti")
	object leftouter extends JoinType("leftouter")
	object rightouter extends JoinType("rightouter")
	object leftsemi extends JoinType("leftsemi")

}
