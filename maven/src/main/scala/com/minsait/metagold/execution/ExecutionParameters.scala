package com.minsait.metagold.execution

object ExecutionParameters {
  sealed case class ExecutionParameter (shortName: String, longName: String, mandatory: Boolean, description: String)

  object AdfRunId extends ExecutionParameter("AR", "adf-runid", false, "Execution pipeline id")
  object TransformActivity extends ExecutionParameter("TAC", "transformation-activity", false, "Transformation activity")
  object TransformActivityByPath extends ExecutionParameter("TAP", "transformation-activity-bypath", false, "Transformation activity by path")
  object ConfigFile extends ExecutionParameter("CF", "config-file", true, "Configuration file")
  object PublishStatistics extends ExecutionParameter("PS", "publish-statistics", false, "Publish statistics")
  object ValidateTransformations extends ExecutionParameter("VT", "validate-transformations", false, "Json Validator")
  object ValidateActivities extends ExecutionParameter("VA", "validate-activities", false, "Json Validator")
  object ValidateSQLConnections extends ExecutionParameter("VC", "validate-connections", false, "Json Validator")

  val executionParameters = List(AdfRunId, TransformActivity, TransformActivityByPath, ConfigFile, PublishStatistics, ValidateTransformations, ValidateActivities, ValidateSQLConnections)
}
