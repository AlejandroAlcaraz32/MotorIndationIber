package com.minsait.indation.metadata.models.enums

object QualityRulesModes {
	sealed case class QualityRulesMode(value: String)
	object RuleWarning extends QualityRulesMode("warning")
	object RuleReject extends QualityRulesMode("reject")
}
