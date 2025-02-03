CREATE TABLE IF NOT EXISTS metalog.ingestion(
    ingestionId SERIAL PRIMARY KEY,
    layer VARCHAR(6) NOT NULL,
    executionStart timestamp(3) NOT NULL,
    executionEnd timestamp(3),
    duration int,
    status varchar(10) NOT NULL,
    result varchar(25)
);

CREATE TABLE IF NOT EXISTS metalog.source_log(
    sourceLogId SERIAL PRIMARY KEY,
    ingestionId int NOT NULL references ingestion(ingestionId),
    sourceId int NOT NULL references metadata.source(sourceId),
    executionStart timestamp(3) NOT NULL,
    executionEnd timestamp(3),
    duration int,
    status varchar(10) NOT NULL,
    result varchar(25)
);

CREATE TABLE IF NOT EXISTS metalog.dataset_log(
    datasetLogId SERIAL PRIMARY KEY,
    sourceLogId int NOT NULL references source_log(sourceLogId),
    specificationId int NOT NULL references metadata.specification(specificationId),
    executionStart timestamp(3) NOT NULL,
    executionEnd timestamp(3),
    duration int,
    status varchar(10) NOT NULL,
    result varchar(25),
    resultMessage text,
    partition varchar(20),
    reprocessed varchar(10),
    rowsOk int,
    rowsKO int,
    outputPath varchar(250),
    archivePath varchar(250),
    errorPath varchar(250),
    pendingSilver boolean,
    bronzeDatasetLogId int
);