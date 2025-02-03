package com.minsait.metagold.metadata.models.enums

object QualityRulesModes {
	sealed case class QualityRulesMode(value: String)
	object RuleWarning extends QualityRulesMode("warning")
	object RuleReject extends QualityRulesMode("reject")
}
