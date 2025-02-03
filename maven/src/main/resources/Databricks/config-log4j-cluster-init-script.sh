#!/bin/bash
echo "Executing on Driver: $DB_IS_DRIVER"
if [[ $DB_IS_DRIVER = "TRUE" ]]; then
LOG4J_PATH="/home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j.properties"
else
LOG4J_PATH="/home/ubuntu/databricks/spark/dbconf/log4j/executor/log4j.properties"
fi

echo "Restore log4j.properties"
if test -f "${LOG4J_PATH}.backup"; then
    cp ${LOG4J_PATH}.backup ${LOG4J_PATH} 
fi

echo "Backup log4j.properties"
cp ${LOG4J_PATH} ${LOG4J_PATH}.backup

echo "Adjusting log4j.properties here: ${LOG4J_PATH}"
echo "log4j.additivity.indationFile=false" >> ${LOG4J_PATH}
echo "log4j.appender.indationFile=com.databricks.logging.RedactionRollingFileAppender" >> ${LOG4J_PATH}
echo "log4j.appender.indationFile.layout=org.apache.log4j.PatternLayout" >> ${LOG4J_PATH}
echo "log4j.appender.indationFile.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %X{prefix} %m%n" >> ${LOG4J_PATH}
echo "log4j.appender.indationFile.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy" >> ${LOG4J_PATH}
echo "log4j.appender.indationFile.rollingPolicy.FileNamePattern=logs/log4j-indation-%d{yyyy-MM-dd-HH}.log.gz" >> ${LOG4J_PATH}
echo "log4j.appender.indationFile.rollingPolicy.ActiveFileName=logs/log4j-indation-active.log" >> ${LOG4J_PATH}
echo "log4j.logger.com.minsait.indation=INFO, indationFile" >> ${LOG4J_PATH}

