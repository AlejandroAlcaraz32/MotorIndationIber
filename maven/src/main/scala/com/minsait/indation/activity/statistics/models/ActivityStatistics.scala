package com.minsait.indation.activity.statistics.models

case class ActivityStatistics(
                               uuid: String,
                               trigger: ActivityTrigger,
                               engine: Option[ActivityEngine],
                               dataset: Option[ActivityDataset],
                               origin: String,
                               result: ActivityResults.ActivityResult,
                               execution: ActivityExecution,
                               output_paths: ActivityOutputPaths,
                               rows: Option[ActivityRows],
                               silver_persistence: Option[ActivitySilverPersistence]
                             )

