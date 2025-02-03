package com.minsait.indation.metadata

import com.minsait.indation.metadata.exceptions.{MetadataJsonException, MetadataValueException}
import com.minsait.indation.metadata.models._
import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType
import com.minsait.indation.metadata.models.enums.{ColumnTransformationTypes, ColumnsTypes, CsvHeaderTypes, FileFormatTypes}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonParser}

import java.util.NoSuchElementException
import scala.collection.mutable.ArrayBuffer

/**
 * Class responsible for getting metadata as JSON objects and transform it as case classes instances.
 */
object MetadataJsonParser {

  def getSpecificationInput(inputJson: JsObject): FileInput = {

    try {
      val filePattern = inputJson.fields("filePattern").convertTo[String]
      val typeFormatStr = inputJson.fields("format").asJsObject.fields("value").convertTo[String]
      val fileFormat = FileFormatTypes.FileFormatType(typeFormatStr)
      val formatCsv = getSpecificationInputFormat(inputJson.fields("csv").asJsObject)
      FileInput(fileFormat, filePattern, Some(formatCsv), None, None, None)
    }
    catch {
      case ex: NoSuchElementException =>
        throw new MetadataJsonException("Metadata input has not all required fields", ex.getCause)
    }
  }

  private def getSpecificationInputFormat(formatJson: JsObject): CsvFormat = {

    try {
      val charset = formatJson.fields("charset").convertTo[String]
      val delimiter = formatJson.fields("delimiter").convertTo[String]

      var header: CsvHeaderTypes.CsvHeaderType = CsvHeaderTypes.WithoutHeader
      if (formatJson.fields.contains("header")) {
        val headerStr = formatJson.fields("header").asJsObject.fields("value").convertTo[String]
        header = CsvHeaderTypes.CsvHeaderType(headerStr)
        if (!CsvHeaderTypes.values.contains(header)) {
          throw new MetadataValueException("Value '" + headerStr + "' not valid for header")
        }
      }

      var escapeChar: Option[String] = None
      if (formatJson.fields.contains("escapeChar")) {
        escapeChar = Some(formatJson.fields("escapeChar").convertTo[String])
      }

      val multiline = if (formatJson.fields.contains("multiline")) Some(formatJson.fields("multiline").convertTo[Boolean])
                      else Some(false)

      val lineSep = if (formatJson.fields.contains("lineSep")) Some(formatJson.fields("lineSep").convertTo[String])
      else None

      val nullValue = if (formatJson.fields.contains("nullValue")) Some(formatJson.fields("nullValue").convertTo[String])
      else None

      CsvFormat(charset, delimiter, header, escapeChar, multiline, lineSep, nullValue)
    }
    catch {
      case ex: NoSuchElementException =>
        throw new MetadataJsonException("Metadata input format has not all required fields", ex.getCause)
    }
  }

  private def getSpecificationInputFormatXls(formatJson: JsObject): XlsFormat = {

    try {
      val sheet = if (formatJson.fields.contains("sheet")) Some(formatJson.fields("sheet").convertTo[String])
      else None

      val dataRange = if (formatJson.fields.contains("dataRange")) Some(formatJson.fields("dataRange").convertTo[String])
      else None

      var header: CsvHeaderTypes.CsvHeaderType = CsvHeaderTypes.WithoutHeader
      if (formatJson.fields.contains("header")) {
        val headerStr = formatJson.fields("header").asJsObject.fields("value").convertTo[String]
        header = CsvHeaderTypes.CsvHeaderType(headerStr)
        if (!CsvHeaderTypes.values.contains(header)) {
          throw new MetadataValueException("Value '" + headerStr + "' not valid for header")
        }
      }

      XlsFormat(header, sheet, dataRange)
    }
    catch {
      case ex: NoSuchElementException =>
        throw new MetadataJsonException("Metadata input format has not all required fields", ex.getCause)
    }
  }

  def getSpecificationSchema(schemaJson: JsObject): SchemaColumns = {

    val columnsArray = new ArrayBuffer[Column]()

    val columnsJsArray = schemaJson.fields.getOrElse("columns", JsonParser("[]"))
    for (col <- columnsJsArray.convertTo[Array[JsObject]]) {
      columnsArray += getColumn(col)
    }

    SchemaColumns(columnsArray.toList)
  }

  private def getColumn(columnJson: JsObject): Column = {

    var name = ""
    try {
      name = columnJson.fields("name").convertTo[String]
    }
    catch {
      case ex: NoSuchElementException =>
        throw new MetadataJsonException("Metadata schema column has not all required fields", ex.getCause)
    }

    var colType: Option[ColumnsType] = None
    if (columnJson.fields.contains("typ")) {
      val colTypeStr = columnJson.fields("typ").asJsObject.fields("value").convertTo[String]
      colType = Some(ColumnsTypes.ColumnsType(colTypeStr))
      if (!ColumnsTypes.values.contains(colType.get)) {
        throw new MetadataValueException("Value '" + colTypeStr + "' not valid for column type")
      }
    }

    val description = if (columnJson.fields.contains("description")) {
      Some(columnJson.fields("description").convertTo[String])
    } else {
      None
    }

    var comment: Option[String] = None
    if (columnJson.fields.contains("comment")) {
      comment = Some(columnJson.fields("comment").convertTo[String])
    }

    var isPrimaryKey: Option[Boolean] = None
    if (columnJson.fields.contains("isPrimaryKey")) {
      isPrimaryKey = Some(columnJson.fields("isPrimaryKey").convertTo[Boolean])
    }

    var isTimestamp: Option[Boolean] = None
    if (columnJson.fields.contains("isTimestamp")) {
      isTimestamp = Some(columnJson.fields("isTimestamp").convertTo[Boolean])
    }

    var isPartitionable: Option[Boolean] = None
    if (columnJson.fields.contains("isPartitionable")) {
      isPartitionable = Some(columnJson.fields("isPartitionable").convertTo[Boolean])
    }

    var transformation: Option[ColumnTransformation] = None
    if (columnJson.fields.contains("transformation")) {
      transformation = Some(getColumnTransformation(columnJson.fields("transformation").asJsObject))
    }

    var decimalParameters: Option[ColumnDecimalParameters] = None
    if (columnJson.fields.contains("decimalParameters")) {
      decimalParameters = Some(getColumnDecimalParameters(columnJson.fields("decimalParameters").asJsObject))
    }

    var sensitive: Boolean = false
    if (columnJson.fields.contains("sensitive")) {
      sensitive = columnJson.fields("sensitive").convertTo[Boolean]
    }

    Column(name, colType.get, description, comment, isPrimaryKey, isTimestamp, isPartitionable, transformation, decimalParameters, sensitive)
  }

  private def getColumnTransformation(colTransJson: JsObject): ColumnTransformation = {

    var typeFormat = ""
    try {
      typeFormat = colTransJson.fields("typ").asJsObject.fields("value").convertTo[String]
    }
    catch {
      case ex: NoSuchElementException =>
        throw new MetadataJsonException("Metadata schema column transformation has not all required fields", ex.getCause)
    }

    var pattern: Option[String] = None
    if (colTransJson.fields.contains("pattern")) {
      val patternStr = colTransJson.fields("pattern").convertTo[String]
      pattern = Some(patternStr)
    }

    ColumnTransformation(ColumnTransformationTypes.ColumnTransformationType(typeFormat), pattern, None)
  }

  private def getColumnDecimalParameters(colDecParam: JsObject): ColumnDecimalParameters = {

    try {
      val precision = colDecParam.fields("precision").convertTo[Int]
      val scale = colDecParam.fields("scale").convertTo[Int]
      ColumnDecimalParameters(precision, scale)
    }
    catch {
      case ex: NoSuchElementException =>
        throw new MetadataJsonException("Metadata schema column decimal parameters has not all required fields", ex.getCause)
    }
  }
}
