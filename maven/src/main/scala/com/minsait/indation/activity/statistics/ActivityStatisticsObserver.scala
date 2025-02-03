package com.minsait.indation.activity.statistics

import com.minsait.common.configuration.ConfigurationReader
import com.minsait.indation.activity.statistics.models.ActivityStatistics

trait ActivityStatisticsObserver extends ConfigurationReader {
  def receiveStatistics(activityStatistics: ActivityStatistics)
}
