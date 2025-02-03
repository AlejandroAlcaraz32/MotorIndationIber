CREATE TABLE IF NOT EXISTS metadata.source(
        sourceId SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description VARCHAR(250),
        typ VARCHAR(10) NOT NULL
);
