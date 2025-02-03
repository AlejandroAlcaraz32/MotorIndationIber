package com.minsait.common.utils.fs

import com.minsait.common.configuration.models.EnvironmentTypes._

object FSCommandsFactory {
  def getFSCommands (executionEnvironment: EnvironmentType): FSCommands = {
    executionEnvironment match {
      case Databricks => DatabricksFSCommands
      case Synapse => SynapseFSCommands
      case ManagedIdentity => SynapseFSCommands
      case Local => LocalFSCommands
      case _ => throw new UnsupportedOperationException(s"Execution envirnomnent $executionEnvironment not supported in FSCommandsFactory")
    }
  }
}
