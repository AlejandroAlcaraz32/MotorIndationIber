CREATE TABLE IF NOT EXISTS metadata.source(
        sourceId SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description VARCHAR(250),
        typ VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata.dataset(
        datasetId SERIAL PRIMARY KEY,
        sourceId INTEGER NOT NULL REFERENCES source(sourceId),
        name VARCHAR(100) NOT NULL,
        description VARCHAR(250),
        typ VARCHAR(10) NOT NULL,
        createDatabase BOOLEAN default false,
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

CREATE TABLE IF NOT EXISTS metadata.specification(
        specificationId SERIAL PRIMARY KEY,
        datasetId INTEGER NOT NULL REFERENCES dataset(datasetId),
        fileInput TEXT NOT NULL,
        schemaDefinition VARCHAR(20),
        schemaFile VARCHAR(250),
        schemaColumns TEXT NOT NULL,
        enabled BOOLEAN NOT NULL,
        effectiveDate TIMESTAMP NOT NULL
);
