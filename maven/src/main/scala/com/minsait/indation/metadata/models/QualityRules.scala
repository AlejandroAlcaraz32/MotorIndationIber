package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.QualityRulesModes.QualityRulesMode

case class QualityRules(
                         uniquenessRule: Option[UniquenessRules],
                         notNullRule: Option[List[String]],
                         integrityRule: Option[IntegrityRules],
                         expressionRule: Option[ExpressionRules],
                         mode: QualityRulesMode)
