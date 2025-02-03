package com.minsait.indation.execution

object ExecutionParameters {
  sealed case class ExecutionParameter(shortName: String, longName: String, mandatory: Boolean, description: String)

  object AdfRunId extends ExecutionParameter("DF", "adf-runid", false, "Execution pipeline id")

  object Activity extends ExecutionParameter("AC", "activity", false, "Execution activity")

  object IngestFile extends ExecutionParameter("IF", "ingest-file", false, "File to ingest")

  object IngestDataset extends ExecutionParameter("ID", "ingest-dataset", false, "Dataset to ingest")

  object DatasetPath extends ExecutionParameter("DP", "dataset-path", false, "Dataset path")

  object IngestSource extends ExecutionParameter("IS", "ingest-source", false, "Source to ingest")

  object SourcePath extends ExecutionParameter("SP", "source-path", false, "Source path")

  object ValidateJson extends ExecutionParameter("VJ", "validate-json", false, "Validate json")

  object ConfigFile extends ExecutionParameter("CF", "config-file", true, "Configuration file")

  object SourceId extends ExecutionParameter("SI", "source-id", false, "Execution source identifier")

  val executionParameters = List(AdfRunId, Activity, IngestFile, IngestDataset, DatasetPath, IngestSource, SourcePath, ValidateJson, ConfigFile, SourceId)
}
