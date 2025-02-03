package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.QualityRulesModes.QualityRulesMode

case class QualityRules(
                         uniquenessRule: Option[UniquenessRules],
                         notNullRule: Option[List[String]],
                         integrityRule: Option[IntegrityRules],
                         expressionRule: Option[ExpressionRules],
                         mode: QualityRulesMode
                       )
