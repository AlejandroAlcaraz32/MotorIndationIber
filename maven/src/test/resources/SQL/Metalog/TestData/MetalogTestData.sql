INSERT INTO metalog.ingestion (ingestionid, layer, executionStart, status) VALUES
    (1, 'BRONZE', '2020-02-13 11:28:51', 'FINISHED');

INSERT INTO metalog.ingestion (ingestionid, layer, executionStart, status) VALUES
    (2, 'BRONZE', '2020-02-13 11:28:51', 'RUNNING');

INSERT INTO metalog.ingestion (ingestionid, layer, executionStart, status) VALUES
    (3, 'SILVER', '2020-02-13 11:28:51', 'RUNNING');

INSERT INTO metalog.ingestion (ingestionid, layer, executionStart, status) VALUES
    (4, 'SILVER', '2020-02-13 11:28:51', 'FINISHED');

INSERT INTO metalog.source_log (sourceLogId, ingestionId, sourceId, executionStart, status) VALUES
    (1, 1, 1, '2020-02-13 11:28:51', 'FINISHED');

INSERT INTO metalog.source_log (sourceLogId, ingestionId, sourceId, executionStart, status) VALUES
    (2, 2, 1, '2020-02-13 11:28:51', 'RUNNING');

INSERT INTO metalog.source_log (sourceLogId, ingestionId, sourceId, executionStart, status) VALUES
    (3, 3, 1, '2020-02-13 11:28:51', 'RUNNING');

INSERT INTO metalog.source_log (sourceLogId, ingestionId, sourceId, executionStart, status) VALUES
    (4, 4, 1, '2020-02-13 11:28:51', 'FINISHED');

INSERT INTO metalog.dataset_log (datasetlogid, sourceLogId, specificationId, executionStart, status, reprocessed, pendingSilver) VALUES
    (1, 1, 1, '2020-02-13 11:28:51', 'FINISHED', 'NONE', true);

INSERT INTO metalog.dataset_log (datasetlogid, sourceLogId, specificationId, executionStart, executionEnd, status, reprocessed, pendingSilver) VALUES
    (2, 1, 1, '2020-02-13 11:28:51', '2020-02-13 11:30:51', 'FINISHED', 'NONE', true);

INSERT INTO metalog.dataset_log (datasetlogid, sourceLogId, specificationId, executionStart, executionEnd, status, reprocessed, pendingSilver, archivePath) VALUES
    (3, 1, 1, '2020-02-13 11:28:51', '2020-02-13 11:30:51', 'FINISHED', 'NONE', false, '/bronze/archive/source1/file1/FILE_20180314.csv');

INSERT INTO metalog.dataset_log (datasetlogid, sourceLogId, specificationId, executionStart, executionEnd, status, result, reprocessed, pendingSilver, bronzeDatasetLogId) VALUES
    (4, 4, 1, '2020-02-13 11:28:51', '2020-02-13 11:30:51', 'FINISHED', 'FINISHED_WITH_ERRORS', 'NONE', false, 3);

INSERT INTO metalog.dataset_log (datasetlogid, sourceLogId, specificationId, executionStart, executionEnd, status, reprocessed, pendingSilver, archivePath) VALUES
    (5, 1, 1, '2020-02-13 11:28:51', '2020-02-13 11:30:51', 'FINISHED', 'NONE', false, '/bronze/archive/source1/file1/FILE_20180315.csv');

INSERT INTO metalog.dataset_log (datasetlogid, sourceLogId, specificationId, executionStart, executionEnd, status, result, reprocessed, pendingSilver, bronzeDatasetLogId) VALUES
    (6, 4, 1, '2020-02-13 11:28:51', '2020-02-13 11:30:51', 'FINISHED', 'FINISHED_OK', 'NONE', false, 5);