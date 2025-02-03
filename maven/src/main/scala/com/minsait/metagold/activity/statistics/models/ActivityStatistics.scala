package com.minsait.metagold.activity.statistics.models

import com.minsait.metagold.activity.statistics.models.ActivityResults.Fail
import com.minsait.metagold.activity.statistics.models.ActivityTriggerTypes.Adf

import java.sql.Timestamp

case class ActivityStatistics(
                               uuid: String="",
                               trigger: ActivityTrigger=ActivityTrigger(Adf,""),
                               engine: ActivityEngine=ActivityEngine("",""),
                               activity: ActivityExecution=ActivityExecution("",List()),
                               activityResult: ActivityResults.ActivityResult=Fail,
                               activityDuration: ActivityDuration=ActivityDuration(new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()),0),
                               transformations: List[ActivityTransformation]=List(),
                               resultMsg: String=""
                             )

