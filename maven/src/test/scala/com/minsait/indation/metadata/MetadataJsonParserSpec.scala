package com.minsait.indation.metadata

import com.minsait.indation.metadata.exceptions.{MetadataJsonException, MetadataValueException}
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums._
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import spray.json.JsonParser

class MetadataJsonParserSpec extends AnyFunSuite with MockitoSugar {

	test("MetadataJsonParser#getSpecificationInput Correct data returns correct SpecInput object") {
		// Given
		val filePattern = "file_<yyyy><mm><dd>.csv"
		val formatType = "csv"
		val charset = "UTF-8"
		val delimiter = "|"
		val headerType = CsvHeaderTypes.FirstLine
		val escapeChar = "/"

		val inputStr = s"""{ "format": { "value": "$formatType" }, "filePattern": "$filePattern", "csv": { "charset": "$charset", "delimiter": "$delimiter", "header": { "value" : "$headerType" }, "escapeChar": "$escapeChar" } }"""
		val specInputFormat = CsvFormat(charset, delimiter, headerType, Some(escapeChar), Some(false), None, None)
		val expectedInput = FileInput(FileFormatTypes.Csv, filePattern, Some(specInputFormat), None, None, None)

		// When
		val actualInput = MetadataJsonParser.getSpecificationInput(JsonParser(inputStr).asJsObject)

		// Then
		assert(actualInput == expectedInput)
	}

	test("MetadataJsonParser#getSpecificationInput Non-existing header type throws MetadataValueException") {
		// Given
		val filePattern = "file_<yyyy><mm><dd>.csv"
		val formatType = "csv"
		val charset = "UTF-8"
		val delimiter = "|"
		val headerType = "wrongHeaderType"
		val escapeChar = "/"

		val inputStr = s"""{ "format": { "value": "$formatType"}, "filePattern": "$filePattern", "csv": { "charset": "$charset", "delimiter": "$delimiter", "header": { "value": "$headerType" }, "escapeChar": "$escapeChar" } }"""

		// When - Then
		assertThrows[MetadataValueException] {
			MetadataJsonParser.getSpecificationInput(JsonParser(inputStr).asJsObject)
		}
	}

	test("MetadataJsonParser#getSpecificationInput Non-existing property should throw MetadataJsonException") {
		// Given
		val partitionBy = "yyyy/mm/dd"
		val ingestionMode = IngestionTypes.FullSnapshot

		val inputStr = s"""{ "partitionBy": "$partitionBy", "ingestionMode": "$ingestionMode" }"""

		// When - Then
		assertThrows[MetadataJsonException] {
			MetadataJsonParser.getSpecificationInput(JsonParser(inputStr).asJsObject)
		}
	}

	test("MetadataJsonParser#getSpecificationInput Non-existing charset property should throw MetadataJsonException") {
		// Given
		val filePattern = "file_<yyyy><mm><dd>.csv"
		val formatType = "csv"
		val delimiter = "|"
		val headerType = CsvHeaderTypes.FirstLine
		val escapeChar = "/"

		val inputStr = s"""{ "format": { "value": "$formatType" }, "filePattern": "$filePattern", "csv": { "delimiter": "$delimiter", "header": { "value": "$headerType" }, "escapeChar": "$escapeChar" } }"""

		// When - Then
		assertThrows[MetadataJsonException] {
			MetadataJsonParser.getSpecificationInput(JsonParser(inputStr).asJsObject)
		}
	}

	test("MetadataJsonParser#getSpecificationSchema Correct data returns correct SpecSchema object") {
		// Given
		val name = "default_name"
		val colType = ColumnsTypes.Decimal
		val description = "standar description 01"
		val comment = "standar comment 01"
		val isPrimaryKey = true
		val transType = ColumnTransformationTypes.Date
		val transPattern = "yyyyMMdd"
		val decPrecision = 5
		val decScale = 2
		val sensitive = true

		val schemaStr =
			s"""{ "columns": [ { "name": "$name", "typ": { "value": "$colType" }, "description": "$description", "comment": "$comment",
				 |"isPrimaryKey": $isPrimaryKey, "transformation": { "typ": { "value": "$transType" }, "pattern": "$transPattern"}, "decimalParameters": { "precision": $decPrecision, "scale": $decScale }, "sensitive": $sensitive } ] }""".stripMargin
		val expectedSchema = SchemaColumns(
			List(
				Column(name, colType,  Some(description), Some(comment), Some(isPrimaryKey), None,None, Some(ColumnTransformation(transType,
					Some(transPattern), None)), Some(ColumnDecimalParameters(decPrecision, decScale)), sensitive = true)
			)
		)

		// When
		val actualSchema = MetadataJsonParser.getSpecificationSchema(JsonParser(schemaStr).asJsObject)

		// Then
		assert(actualSchema.columns.mkString(",") == expectedSchema.columns.mkString(","))
	}

	test("MetadataJsonParser#getSpecificationSchema Non-existing column type throws MetadataValueException") {
		// Given
		val name = "default_name"
		val colType = "wrongType"
		val comment = "standar comment 01"
		val isPrimaryKey = true
		val transType = "date"
		val transPattern = "yyyyMMdd"
		val decPrecision = 5
		val decScale = 2
		val validFrom = "2016-12-01"
		val sensitive = true

		val schemaStr = s"""{ "columns": [ { "name": "$name", "typ": { "value": "$colType" }, "comment": "$comment", "isPrimaryKey": $isPrimaryKey, "transformation": { "type": "$transType", "pattern": "$transPattern"}, "decimalParameters": { "precision": $decPrecision, "scale": $decScale }, "validFrom": "$validFrom", "sensitive": $sensitive } ] }"""

		// When - Then
		assertThrows[MetadataValueException] {
			MetadataJsonParser.getSpecificationSchema(JsonParser(schemaStr).asJsObject)
		}
	}

	test("MetadataJsonParser#getSpecificationSchema Non-existing column property throws MetadataJsonException") {
		// Given
		val colType = ColumnsTypes.Decimal
		val comment = "standar comment 01"
		val isPrimaryKey = true
		val transType = ColumnTransformationTypes.Date
		val transPattern = "yyyyMMdd"
		val decPrecision = 5
		val decScale = 2
		val sensitive = true

		val schemaStr = s"""{ "columns": [ { "typ": { "value": "$colType" }, "comment": "$comment", "isPrimaryKey": $isPrimaryKey, "transformation": { "type": "$transType", "pattern": "$transPattern"}, "decimalParameters": { "precision": $decPrecision, "scale": $decScale }, "sensitive": $sensitive } ] }"""

		// When - Then
		assertThrows[MetadataJsonException] {
			MetadataJsonParser.getSpecificationSchema(JsonParser(schemaStr).asJsObject)
		}
	}

	test("MetadataJsonParser#getSpecificationSchema Non-existing column transformation property throws MetadataJsonException") {
		// Given
		val name = "default_name"
		val colType = ColumnsTypes.Decimal
		val comment = "standar comment 01"
		val isPrimaryKey = true
		val transPattern = "yyyyMMdd"
		val decPrecision = 5
		val decScale = 2
		val sensitive = true

		val schemaStr = s"""{ "columns": [ { "name": "$name", "typ": { "value": "$colType" }, "comment": "$comment", "isPrimaryKey": $isPrimaryKey, "transformation": { "pattern": "$transPattern"}, "decimalParameters": { "precision": $decPrecision, "scale": $decScale }, "sensitive": $sensitive } ] }"""

		// When - Then
		assertThrows[MetadataJsonException] {
			MetadataJsonParser.getSpecificationSchema(JsonParser(schemaStr).asJsObject)
		}
	}

	test("MetadataJsonParser#getSpecificationSchema Non-existing column decimal parameter property throws MetadataJsonException") {
		// Given
		val name = "default_name"
		val colType = ColumnsTypes.Decimal
		val comment = "standar comment 01"
		val isPrimaryKey = true
		val transType = ColumnTransformationTypes.Date
		val transPattern = "yyyyMMdd"
		val decPrecision = 5
		val sensitive = true

		val schemaStr = s"""{ "columns": [ { "name": "$name", "typ": { "value": "$colType" }, "comment": "$comment", "isPrimaryKey": $isPrimaryKey, "transformation": { "typ": { "value": "$transType" }, "pattern": "$transPattern"}, "decimalParameters": { "precision": $decPrecision }, "sensitive": $sensitive } ] }"""

		// When - Then
		assertThrows[MetadataJsonException] {
			MetadataJsonParser.getSpecificationSchema(JsonParser(schemaStr).asJsObject)
		}
	}
}
