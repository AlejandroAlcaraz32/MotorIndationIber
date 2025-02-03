CREATE TABLE IF NOT EXISTS metadata.specification(
        specificationId SERIAL PRIMARY KEY,
        datasetId INTEGER NOT NULL REFERENCES metadata.dataset(datasetId),
        fileInput JSONB NOT NULL,
        schemaDefinition VARCHAR(20),
        schemaFile VARCHAR(250),
        schemaColumns JSONB NOT NULL,
        enabled BOOLEAN NOT NULL,
        effectiveDate TIMESTAMP NOT NULL
);
