CREATE TABLE IF NOT EXISTS metalog.source_log(
    sourceLogId SERIAL PRIMARY KEY,
    ingestionId int NOT NULL references metalog.ingestion(ingestionId),
    sourceId int NOT NULL references metadata.source(sourceId),
    executionStart timestamp(3) NOT NULL,
    executionEnd timestamp(3),
    duration int,
    status varchar(10) NOT NULL,
    result varchar(25)
);