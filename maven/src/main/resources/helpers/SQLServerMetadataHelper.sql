-- Variables para elegir la tabla
declare @esquema varchar(100) = 'dbo' -- poner el esquema donde está la tabla a parsear
declare @tabla varchar(100) = 'his_nc_noconformidades' -- poner la tabla a parsear aquí
declare @source varchar(100) = 'mapex_ger' -- poner el origen de metadata (nombre del JSON de origen)
declare @silverDatabase varchar(100) = 'slv_mapex' -- poner aquí la base de datos o agrupación Silver
declare @silverTable varchar(100) = 'his_nc_noconformidades' -- poner aquí el nombre de la tabla de destino en Silver

-- Código de generación de Json
declare C2 CURSOR FOR
	select
		COLUMN_NAME as Columna,

		case (data_type)
			when 'int' then 'integer'
			when 'bigint' then 'long'
			when 'date' then 'date'
			when 'datetime' then 'datetime'
			when 'datetime2' then 'datetime'
			when 'smalldatetime' then 'datetime'
			when 'nchar' then 'string'
			when 'char' then 'string'
			when 'numeric' then 'decimal'
			when 'decimal' then 'decimal'
			when 'smallint' then 'short'
			when 'tinyint' then 'short'
			when 'varchar' then 'string'
			when 'nvarchar' then 'string'
			when 'float' then 'double'
			when 'xml' then 'string'
			when 'varbinary' then 'binary'
			when 'bit' then 'boolean'
		end as TIPO,
		col.ORDINAL_POSITION as posicion,
		'false' as EsClavePrimaria,
		'false' as EsTimestamp,
		'false' as EsParticionable,
		'false' as Sensitive,
		NUMERIC_PRECISION AS PRECISION,
		NUMERIC_SCALE AS DECIMALES,
		CHARACTER_MAXIMUM_LENGTH AS Longitud
		--select *
	from INFORMATION_SCHEMA.COLUMNS col
	where TABLE_NAME like @tabla and table_schema like @esquema;
declare @Columna varchar(4000)
declare @Tipo varchar(4000)
declare @Posicion varchar(4000)
declare @ClavePrincipal varchar(4000)
declare @EsTimestamp varchar(4000)
declare @EsParticionable varchar(4000)
declare @Sensitive varchar(4000)
declare @Precision varchar(4000)
declare @Decimales varchar(4000)
declare @Longitud varchar(4000)
declare @SiguienteComa varchar(1)=''

begin
	-- bloque de tabla
	print('{')

	print('  "name": "' + @silverTable + '",')
	print('  "description": "'+ @silverTable + '",')
	print('  "sourceName": "' + @source+'",')
	print('  "typ": { "value": "table" },')
	print('  "version": 1,')
	print('  "enabled": true,')
	print('  "classification": { "value": "public" },')
	print('  "effectiveDate": "2020-01-01 00:00:00",')
	print('  "ingestionMode": { "value": "full_snapshot" },')
	print('  "validationMode": { "value": "fail_fast" },')
	print('  "createDatabase": true,')
	print('  "database": "' + @silverDatabase + '",')
	print('  "table": "'+@silverTable+'",')
	print('  "partitionBy": "",')
	print('  "permissiveThresholdType": {"value" :"absolute"},')
	print('  "permissiveThreshold": 0,')
	print('  "tableInput":{')
	print('    "table": "'+@tabla+'",')
	print('    "schema": "'+@esquema+'"')
	print('  },')
	print('  "schemaDefinition": {"value": "json-columns"},')
	print('  "schemaFile": "",')

	-- inicio bloque de columnas
	print('  "schemaColumns": {')
	print('    "columns": [')

	-- bloque de columnas recursivo
	OPEN C2;
	FETCH NEXT FROM C2 INTO
		@Columna,
		@Tipo,
		@Posicion,
		@ClavePrincipal,
		@EsTimestamp,
		@EsParticionable,
		@Sensitive,
		@Precision,
		@Decimales,
		@Longitud

	WHILE @@FETCH_STATUS = 0
	BEGIN
		print('    ' + @SiguienteComa + '{')
		print('      "typ": { "value": "'+ @Tipo + '"},')
		print('      "name": "' + @Columna + '",')
		print('      "description": "' + @Columna + '",')
		print('      "position": ' + @Posicion + ',')
		print('      "dictionaryLinkedName": "",')
		print('      "isPrimaryKey": ' + @ClavePrincipal + ',')
		print('      "isTimestamp": ' + @EsTimestamp + ',')
		print('      "isPartitionable": ' + @EsParticionable + ',')
		print('      "sensitive": ' + @Sensitive + '')
		if (@Tipo = 'string')
		begin
			print('      ,"length": ' + @Longitud )
		end
		if (@Tipo = 'decimal')
		begin
			print('      ,"decimalParameters": {')
			print('        "precision": ' + @Precision + ',')
			print('        "scale": ' + @Decimales )
			print('      }')
		end
		print('     }')
		FETCH NEXT FROM C2 INTO
			@Columna,
			@Tipo,
			@Posicion,
			@ClavePrincipal,
			@EsTimestamp,
			@EsParticionable,
			@Sensitive,
			@Precision,
			@Decimales,
			@Longitud
		set @SiguienteComa=','
	END;
	CLOSE C2;
	DEALLOCATE C2;

	-- fin columnas
	print('    ]')
	print('  }')

	-- fin tabla
	print('  }')

end




