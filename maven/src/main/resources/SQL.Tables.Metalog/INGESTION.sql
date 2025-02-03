CREATE TABLE IF NOT EXISTS metalog.ingestion(
    ingestionId SERIAL PRIMARY KEY,
    layer VARCHAR(6) NOT NULL,
    executionStart timestamp(3) NOT NULL,
    executionEnd timestamp(3),
    duration int,
    status varchar(10) NOT NULL,
    result varchar(25)
);