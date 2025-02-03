package com.minsait.metagold.activity.statistics

import com.minsait.metagold.activity.statistics.models.ActivityStatistics
import com.minsait.common.configuration.ConfigurationReader

trait ActivityStatisticsObserver extends ConfigurationReader {
  def receiveStatistics(activityStatistics: ActivityStatistics)
}
