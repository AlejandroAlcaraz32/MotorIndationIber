INSERT INTO metadata.source (name, description, typ) VALUES ('source1', 'source1 default description', 'directory');
INSERT INTO metadata.source (name, description, typ) VALUES ('source2', '', 'directory');
INSERT INTO metadata.source (name, typ) VALUES ('source3', 'directory');

INSERT INTO metadata.dataset (sourceId, name, description, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (1, 'dataset1', 'dataset1 default description', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot', 'database1', 'tableName1', 'permissive', 2, 'absolute');
INSERT INTO metadata.dataset (sourceId, name, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (1, 'dataset2', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot','database1', 'tableName1', 'permissive', 2, 'ABSOLUTE');
INSERT INTO metadata.dataset (sourceId, name, description, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (2, 'dataset3', 'dataset3 default description', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot', 'database3', 'tableName3', 'permissive', 2, 'absolute');
INSERT INTO metadata.dataset (sourceId, name, description, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (3, 'dataset4', 'dataset4 default description', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot', 'database3', 'tableName4', 'permissive', 2, 'absolute');
INSERT INTO metadata.dataset (sourceId, name, description, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (3, 'dataset5', 'dataset5 default description', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot', 'database3', 'tableName5', 'permissive', 2, 'absolute');
INSERT INTO metadata.dataset (sourceId, name, description, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (3, 'dataset6', 'dataset6 default description', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot', 'database3', 'tableName6', 'permissive', 2, 'absolute');
INSERT INTO metadata.dataset (sourceId, name, description, typ, createdatabase, classification, partitionBy, ingestionMode, dataBase, tableName, validationMode, permissiveThreshold, permissiveThresholdType) VALUES (3, 'dataset7', 'dataset7 default description', 'file', true, 'public', 'yyyy/mm/dd', 'full_snapshot', 'database3', 'tableName7', 'permissive', 2, 'absolute');

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        1,
        '{ "format": {"value": "csv"}, "filePattern": "file1_<yyyy><mm><dd>.csv", "csv": { "charset": "UTF-8", "delimiter": "|", "header": { "value": "first_line"} } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }}, {"name": "column2", "typ": { "value": "string" }}, {"name": "column3", "typ": { "value": "integer" }}, {"name": "column4", "typ": { "value": "integer" }}, {"name": "column5", "typ": { "value": "integer" }}, {"name": "column6", "typ": { "value": "string" }}, {"name": "column7", "typ": { "value": "integer" }}, {"name": "column8", "typ": { "value": "integer" }}, {"name": "column9", "typ": { "value": "string" }}, {"name": "column10", "typ": { "value": "string" }}, {"name": "column11", "typ": { "value": "string" }}, {"name": "column12", "typ": { "value": "integer" }}, {"name": "column13", "typ": { "value": "string" }}, {"name": "column14", "typ": { "value": "integer" }}, {"name": "column15", "typ": { "value": "integer" }}, {"name": "column16", "typ": { "value": "string" }}, {"name": "column17", "typ": { "value": "string" }}, {"name": "column18", "typ": { "value": "string" }}, {"name": "column19", "typ": { "value": "integer" }}, {"name": "column20", "typ": { "value": "integer" }}, {"name": "column21", "typ": { "value": "integer" }}, {"name": "column22", "typ": { "value": "integer" }}, {"name": "column23", "typ": { "value": "string" }}, {"name": "column24", "typ": { "value": "integer" }}, {"name": "column25", "typ": { "value": "integer" }}, {"name": "column26", "typ": { "value": "string" }}, {"name": "column27", "typ": { "value": "string" }}, {"name": "column28", "typ": { "value": "string" }}, {"name": "column29", "typ": { "value": "string" }}, {"name": "column30", "typ": { "value": "string" }}, {"name": "column31", "typ": { "value": "string" }}, {"name": "column32", "typ": { "value": "string" }}, {"name": "column33", "typ": { "value": "string" }}, {"name": "column34", "typ": { "value": "string" }}, {"name": "column35", "typ": { "value": "integer" }}, {"name": "column36", "typ": { "value": "integer" }}, {"name": "column37", "typ": { "value": "string" }}, {"name": "column38", "typ": { "value": "string" }}, {"name": "column39", "typ": { "value": "double" }}, {"name": "column40", "typ": { "value": "integer" }}, {"name": "column41", "typ": { "value": "integer" }}, {"name": "column42", "typ": { "value": "integer" }}, {"name": "column43", "typ": { "value": "double" }}, {"name": "column44", "typ": { "value": "double" }}, {"name": "column45", "typ": { "value": "double" }}, {"name": "column46", "typ": { "value": "double" }}, {"name": "column47", "typ": { "value": "double" }}, {"name": "column48", "typ": { "value": "double" }}, {"name": "column49", "typ": { "value": "double" }}, {"name": "column50", "typ": { "value": "double" }}, {"name": "column51", "typ": { "value": "double" }}, {"name": "column52", "typ": { "value": "double" }}, {"name": "column53", "typ": { "value": "double" }}, {"name": "column54", "typ": { "value": "double" }}, {"name": "column55", "typ": { "value": "double" }}, {"name": "column56", "typ": { "value": "double" }}, {"name": "column57", "typ": { "value": "decimal" }, "decimalParameters": {"precision": 4, "scale": 2 } }, {"name": "column", "typ": { "value": "double" }}, {"name": "column58", "typ": { "value": "integer" }}, {"name": "column59", "typ": { "value": "integer" }}, {"name": "column61", "typ": { "value": "integer" }}, {"name": "column62", "typ": { "value": "string" }}, {"name": "column63", "typ": { "value": "integer" }}, {"name": "column64", "typ": { "value": "integer" }}, {"name": "column65", "typ": { "value": "string" }}, {"name": "column66", "typ": { "value": "integer" }}, {"name": "column67", "typ": { "value": "string" }}, {"name": "column68", "typ": { "value": "integer" }}, {"name": "column69", "typ": { "value": "string" }}, {"name": "column70", "typ": { "value": "integer" }}, {"name": "column71", "typ": { "value": "integer" }}, {"name": "column72", "typ": { "value": "integer" }}, {"name": "column73", "typ": { "value": "integer" }}, {"name": "column74", "typ": { "value": "integer" }}, {"name": "column75", "typ": { "value": "integer" }}, {"name": "column76", "typ": { "value": "integer" }}, {"name": "column77", "typ": { "value": "integer" }}, {"name": "column78", "typ": { "value": "integer" }}, {"name": "column79", "typ": { "value": "integer" }}, {"name": "column80", "typ": { "value": "integer" }}, {"name": "column81", "typ": { "value": "integer" }}, {"name": "column82", "typ": { "value": "integer" }}, {"name": "column83", "typ": { "value": "integer" }}, {"name": "column84", "typ": { "value": "integer" }}, {"name": "column85", "typ": { "value": "integer" }}, {"name": "column86", "typ": { "value": "integer" }}, {"name": "column87", "typ": { "value": "string" }}, {"name": "column88", "typ": { "value": "string" }}, {"name": "column89", "typ": { "value": "integer" }}, {"name": "column90", "typ": { "value": "string" }}, {"name": "column91", "typ": { "value": "integer" }}, {"name": "column92", "typ": { "value": "integer" }}, {"name": "column93", "typ": { "value": "integer" }}, {"name": "column94", "typ": { "value": "double" }}, {"name": "column95", "typ": { "value": "double" }}, {"name": "column96", "typ": { "value": "double" }}, {"name": "column97", "typ": { "value": "double" }}, {"name": "column98", "typ": { "value": "integer" }}, {"name": "column99", "typ": { "value": "integer" }}, {"name": "column100", "typ": { "value": "integer" }}, {"name": "column101", "typ": { "value": "integer" }}, {"name": "column102", "typ": { "value": "integer" }}, {"name": "column103", "typ": { "value": "integer" }}, {"name": "column104", "typ": { "value": "string" }}, {"name": "column105", "typ": { "value": "string" }}, {"name": "column106", "typ": { "value": "string" }}, {"name": "column107", "typ": { "value": "integer" }}, {"name": "column108", "typ": { "value": "string" }}, {"name": "column109", "typ": { "value": "integer" }}, {"name": "column110", "typ": { "value": "integer" }}, {"name": "column111", "typ": { "value": "string" }}, {"name": "column112", "typ": { "value": "string" }}, {"name": "column113", "typ": { "value": "string" }}, {"name": "column114", "typ": { "value": "integer" }}, {"name": "column115", "typ": { "value": "integer" }}, {"name": "column116", "typ": { "value": "string" }}, {"name": "column117", "typ": { "value": "string" }}, {"name": "column118", "typ": { "value": "integer" }}, {"name": "column119", "typ": { "value": "string" }}, {"name": "column120", "typ": { "value": "string" }}, {"name": "column121", "typ": { "value": "string" }, "validFrom": "2016-12-01"}, {"name": "column122", "typ": { "value": "string" }, "validFrom": "2016-12-01"} ] }',
        true,
        '2020-01-01 00:00:00'
    );

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        2,
        '{ "format": {"value": "csv"}, "filePattern": "file2_<yyyy><mm><dd>.csv", "csv": { "charset": "UTF-8", "delimiter": "|", "header": { "value": "first_line"} } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "string" }}, {"name": "column2", "typ": { "value": "string" }}, {"name": "column3", "typ": { "value": "datetime" }, "transformation": {"typ": { "value": "datetime" }, "pattern": "yyyyMMdd"} }, {"name": "column4", "typ": { "value": "datetime" }, "transformation": {"typ": { "value": "datetime" }, "pattern": "yyyyMMdd"} }, {"name": "column5", "typ": { "value": "string" }}, {"name": "column6", "typ": { "value": "string" }}, {"name": "column7", "typ": { "value": "string" }}, {"name": "column8", "typ": { "value": "string" }}, {"name": "column9", "typ": { "value": "datetime" }, "transformation": {"typ": { "value": "datetime" }, "pattern": "yyyyMMdd"} }, {"name": "column10", "typ": { "value": "string" }}, {"name": "column11", "typ": { "value": "string" }}, {"name": "column12", "typ": { "value": "datetime" }, "transformation": {"typ": { "value": "datetime" }, "pattern": "yyyyMMdd"} }, {"name": "column13", "typ": { "value": "datetime" }, "transformation": {"typ": { "value": "datetime" }, "pattern": "yyyyMMdd"} }, {"name": "column14", "typ": { "value": "double" }}, {"name": "column15", "typ": { "value": "integer" }}, {"name": "column16", "typ": { "value": "integer" }}, {"name": "column17", "typ": { "value": "integer" }}, {"name": "column18", "typ": { "value": "integer" }}, {"name": "column19", "typ": { "value": "double" }}, {"name": "column20", "typ": { "value": "double" }}, {"name": "column21", "typ": { "value": "double" }}, {"name": "column22", "typ": { "value": "double" }}, {"name": "column23", "typ": { "value": "double" }}, {"name": "column24", "typ": { "value": "double" }}, {"name": "column25", "typ": { "value": "double" }}, {"name": "column26", "typ": { "value": "double" }}, {"name": "column27", "typ": { "value": "double" }}, {"name": "column28", "typ": { "value": "integer" }}, {"name": "column29", "typ": { "value": "integer" }}, {"name": "column30", "typ": { "value": "integer" }}, {"name": "column31", "typ": { "value": "string" }}, {"name": "column32", "typ": { "value": "string" }}, {"name": "column33", "typ": { "value": "string" }}, {"name": "column34", "typ": { "value": "string" }}, {"name": "column35", "typ": { "value": "string" }}, {"name": "column36", "typ": { "value": "string" }}, {"name": "column37", "typ": { "value": "string" }}, {"name": "column38", "typ": { "value": "string" }}, {"name": "column39", "typ": { "value": "string" }}, {"name": "column40", "typ": { "value": "string" }}, {"name": "column41", "typ": { "value": "string" }}, {"name": "column42", "typ": { "value": "string" }}, {"name": "column43", "typ": { "value": "string" }}, {"name": "column44", "typ": { "value": "string" }}, {"name": "column45", "typ": { "value": "string" }}, {"name": "column46", "typ": { "value": "string" }}, {"name": "column47", "typ": { "value": "string" }}, {"name": "column48", "typ": { "value": "string" }}, {"name": "column49", "typ": { "value": "string" }}, {"name": "column50", "typ": { "value": "string" }}, {"name": "column51", "typ": { "value": "string" }} ] }',
        true,
        '2020-01-01 00:00:00'
    );


INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        3,
        '{ "format": {"value": "csv"}, "filePattern": "file3_<yyyy><mm><dd>.csv", "csv": { "charset": "US-ASCII", "delimiter": "|" } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }, "isPrimaryKey": true }, {"name": "column2", "typ": { "value": "string" }} ] }',
        true,
        '2020-01-01 00:00:00'
    );

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        4,
        '{ "format": {"value": "csv"}, "filePattern": "file4_<yyyy><mm><dd>.csv", "csv": { "charset": "US-ASCII", "delimiter": "|" } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }, "isPrimaryKey": true }, {"name": "column2", "typ": { "value": "string" }} ] }',
        true,
        '2020-01-01 00:00:00'
    );

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        5,
        '{ "format": {"value": "csv"}, "filePattern": "file4_<yyyy><mm><dd>.csv", "csv": { "charset": "US-ASCII", "delimiter": "|" } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }, "isPrimaryKey": true }, {"name": "column2", "typ": { "value": "string" }} ] }',
        true,
        '2020-01-01 00:00:00'
    );

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        6,
        '{ "format": {"value": "csv"}, "filePattern": "file6_<yyyy><mm><dd>.csv", "csv": { "charset": "US-ASCII", "delimiter": "|" } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }, "isPrimaryKey": true }, {"name": "column2", "typ": { "value": "string" }} ] }',
        true,
        '2020-01-01 00:00:00'
    );

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        6,
        '{ "format": {"value": "json"}, "filePattern": "file6_<yyyy><mm><dd>.csv", "csv": { "charset": "US-ASCII", "delimiter": "|" } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }, "isPrimaryKey": true }, {"name": "column2", "typ": { "value": "string" }} ] }',
        true,
        '2020-07-01 00:00:00'
    );

INSERT INTO metadata.specification (datasetId, fileInput, schemaColumns, enabled, effectiveDate) VALUES
    (
        7,
        '{ "format": {"value": "json"}, "filePattern": "file7_<yyyy><mm><dd>.csv", "csv": { "charset": "US-ASCII", "delimiter": "|" } }',
        '{ "columns": [{"name": "column1", "typ": { "value": "integer" }, "isPrimaryKey": true }, {"name": "column2", "typ": { "value": "string" }} ] }',
        true,
        '3000-07-01 00:00:00'
    );