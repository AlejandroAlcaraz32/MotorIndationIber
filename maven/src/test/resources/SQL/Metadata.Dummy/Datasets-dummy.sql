INSERT INTO metadata.source
            (NAME,
             description,
             typ)
VALUES      ('nycgov',
             'New York City Open Data',
             'directory');

INSERT INTO metadata.dataset
            (sourceid,
             NAME,
             description,
             typ,
             createdatabase,
             classification,
             partitionby,
             ingestionmode,
             database,
             tablename,
             validationmode,
             permissivethreshold,
             permissivethresholdtype)
VALUES      (1,
             'yellow_taxi_trip',
             'Yellow Taxi Trip Records',
             'file',
             true,
             'public',
             'yyyy/mm',
             'incremental',
             'dummy',
             'yellow_taxi_trip',
             'fail_fast',
             1,
             'percentage');

INSERT INTO metadata.specification
            (datasetid,
             fileinput,
             schemadefinition,
             schemacolumns,
             enabled,
             effectivedate)
VALUES      ( 1,
'{ "format": {"value": "csv"}, "filePattern": "yellow_tripdata_<yyyy>-<mm>.csv", "csv": { "charset": "UTF-8", "delimiter": ",", "header": { "value": "first_line"} } }'
              ,
'json-columns',
'{ "columns": [
{"name": "VendorID", "typ": { "value": "integer" }},
{"name": "tpep_pickup_datetime", "typ": { "value": "date" }, "transformation": {"typ": { "value": "date" }, "pattern": "yyyy-MM-dd HH:mm:ss"} },
{"name": "tpep_dropoff_datetime", "typ": { "value": "date" }, "transformation": {"typ": { "value": "date" }, "pattern": "yyyy-MM-dd HH:mm:ss"} },
{"name": "passenger_count", "typ": { "value": "integer" }},
{"name": "trip_distance", "typ": { "value": "float" }},
{"name": "RatecodeID", "typ": { "value": "integer" }},
{"name": "store_and_fwd_flag", "typ": { "value": "string" }},
{"name": "PULocationID", "typ": { "value": "integer" }},
{"name": "DOLocationID", "typ": { "value": "integer" }},
{"name": "payment_type", "typ": { "value": "integer" }},
{"name": "fare_amount", "typ": { "value": "float" }},
{"name": "extra", "typ": { "value": "float" }},
{"name": "mta_tax", "typ": { "value": "float" }},
{"name": "tip_amount", "typ": { "value": "float" }},
{"name": "tolls_amount", "typ": { "value": "float" }},
{"name": "improvement_surcharge", "typ": { "value": "float" }},
{"name": "total_amount", "typ": { "value": "float" }},
{"name": "congestion_surcharge", "typ": { "value": "float" }}
] }',
true,
'2020-01-01 00:00:00' );