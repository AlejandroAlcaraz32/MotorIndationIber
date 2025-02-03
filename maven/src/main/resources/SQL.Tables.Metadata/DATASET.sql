CREATE TABLE IF NOT EXISTS metadata.dataset(
        datasetId SERIAL PRIMARY KEY,
        sourceId INTEGER NOT NULL REFERENCES metadata.source(sourceId),
        name VARCHAR(100) NOT NULL,
        description VARCHAR(250),
        typ VARCHAR(10) NOT NULL,
        createDatabase BOOLEAN DEFAULT FALSE,
        classification varchar(10) default 'public',
        partitionBy varchar(50),
        allowPartitionChange BOOLEAN DEFAULT TRUE,
        ingestionMode varchar(50) NOT NULL,
        database varchar(50) NOT NULL,
        tableName varchar(50) NOT NULL,
        validationMode varchar(10)NOT NULL,
        permissiveThreshold INTEGER NOT NULL,
        permissiveThresholdType varchar(50) NOT NULL
);
