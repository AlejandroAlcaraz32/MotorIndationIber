package com.minsait.indation.silver.helper

import com.minsait.indation.metadata.models.enums.ColumnsTypes.ColumnsType
import com.minsait.indation.metadata.models.enums.{ColumnsTypes, SchemaDefinitionTypes}
import com.minsait.indation.metadata.models.{Column, Dataset}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object SchemaHelper {

	def createStructSchema(cols: List[Column], includeMetadata: Boolean = false): StructType = {
		val parentCols = cols.filter(x => x.parentName.isEmpty || x.parentName.get.isEmpty)

		createStructSchema(cols, parentCols, includeMetadata)
	}

	def createStructSchema(cols: List[Column], parentCols: List[Column], includeMetadata: Boolean): StructType = {

		var schema = new StructType()

		for (c <- parentCols) {

			val _type: DataType = getColumnDataType(c, cols, includeMetadata)

			schema = if(includeMetadata){
				val metadata = new MetadataBuilder()
					.putBoolean("sensitive", c.sensitive)

				if(c.alias.isDefined)
					metadata.putString("alias", c.alias.get)

				schema.add(StructField(c.name, _type, nullable = true, metadata.build))
			} else {
				schema.add(StructField(c.name, _type, nullable = true))
			}
		}
		schema
	}

	def getColumnDataType(c: Column, allCols: List[Column], sensitiveMetadata: Boolean): DataType = {
		var _type: DataType = null

		c.typ match {

			case ColumnsTypes.Decimal =>
				_type = DecimalType(c.decimalParameters.get.precision, c.decimalParameters.get.scale)

			case ColumnsTypes.Array => {
				val elementType = c.arrayInfo.get.elementType
				_type = if (elementType == ColumnsTypes.Struct || elementType == ColumnsTypes.Array) {
					val columnName = c.name
					//Solo puede tener una columna interna
					val innerColumn = allCols.filter(col => col.parentName.isDefined && col.parentName.get.equals(columnName)).head

					//TODO: Asegurar que el tipo del hijo es el mismo del padre, es decir, que si el array es de struct, por ejemplo, el hijo no sea string
					ArrayType(getColumnDataType(innerColumn, allCols, sensitiveMetadata))

				}
				else {
					ArrayType(getDataType(elementType))
				}
			}

			case ColumnsTypes.Struct => {
				val columnName = c.name
				val innerColumns = allCols.filter(col => col.parentName.isDefined && col.parentName.get.equals(columnName))

				val fields = innerColumns.map(innerColumn => {
					val colType = getColumnDataType(innerColumn, allCols, sensitiveMetadata)

					if(sensitiveMetadata){
						val metadata = new MetadataBuilder()
							.putBoolean("sensitive", innerColumn.sensitive)
							.build

						StructField(innerColumn.name, colType, nullable = true, metadata)
					} else {
						StructField(innerColumn.name, colType, nullable = true)
					}
				})

				_type = StructType.apply(fields)
			}

			case _ =>
				_type = getDataType(c.typ)
		}

		_type
	}

	def getDataType(columnType: ColumnsType): DataType ={
			columnType match {
				case ColumnsTypes.Integer =>
					IntegerType

				case ColumnsTypes.Short =>
					ShortType

				case ColumnsTypes.String =>
					StringType

				case ColumnsTypes.Float =>
					FloatType

				case ColumnsTypes.Long =>
					LongType

				case ColumnsTypes.Boolean =>
					BooleanType

				case ColumnsTypes.Double =>
					DoubleType

				case ColumnsTypes.Date =>
					DateType

				case ColumnsTypes.DateTime =>
					TimestampType

				case ColumnsTypes.Binary =>
					BinaryType

				case _ => throw new UnsupportedOperationException("Unsupported Column Type " + columnType.value)
			}
	}

	def jsonColumnsStructTypeAllString(spec: Dataset): StructType = {
		StructType(spec.schemaColumns.get.columns.map(col => StructField(col.name, StringType, nullable = true)))
	}

	def structTypeSchema(dataset: Dataset, includeMetadata: Boolean = false): StructType = {
		dataset.schemaDefinition match {
			case SchemaDefinitionTypes.Columns => this.createStructSchema(dataset.schemaColumns.get.columns, includeMetadata)
			case SchemaDefinitionTypes.Json => DataType.fromJson(dataset.schemaJson.get).asInstanceOf[StructType]
			case _ => throw new UnsupportedOperationException("Unsupported schema definition type " + dataset.schemaDefinition.value)
		}
	}

	def primaryKeys(dataset: Dataset): List[String] = {
		dataset.schemaDefinition match {
			case SchemaDefinitionTypes.Columns =>
				dataset.schemaColumns.get.columns.filter(col => col.isPrimaryKey.getOrElse(false)).map(_.name)
			case SchemaDefinitionTypes.Json =>
				this.structTypeSchema(dataset).filter(_.metadata.contains("isPrimaryKey")).filter(_.metadata.getBoolean("isPrimaryKey")).map(_.name).toList
			case _ => throw new UnsupportedOperationException("Unsupported schema definition type " + dataset.schemaDefinition.value)
		}
	}

	def timeStampColumn(dataset: Dataset): String = {
		dataset.schemaDefinition match {
			case SchemaDefinitionTypes.Columns =>
				dataset.schemaColumns.get.columns.filter(col => col.isTimestamp.getOrElse(false)).map(_.name).headOption.getOrElse("")
			case SchemaDefinitionTypes.Json =>
				this.structTypeSchema(dataset).filter(_.metadata.contains("isTimestamp")).filter(_.metadata.getBoolean("isTimestamp")).map(_.name).toList
					.headOption.getOrElse("")
			case _ => throw new UnsupportedOperationException("Unsupported schema definition type " + dataset.schemaDefinition.value)
		}
	}

	def partitionableDateColumn(dataset: Dataset): String={
		dataset.schemaDefinition match {
			case SchemaDefinitionTypes.Columns =>
				dataset.schemaColumns.get.columns.filter(col => col.isPartitionable.getOrElse(false)).map(_.name).headOption.getOrElse("")
			case SchemaDefinitionTypes.Json =>
				this.structTypeSchema(dataset).filter(_.metadata.contains("isPartitionable")).filter(_.metadata.getBoolean("isPartitionable")).map(_.name).toList
					.headOption.getOrElse("")
			case _ => throw new UnsupportedOperationException("Unsupported schema definition type " + dataset.schemaDefinition.value)
		}
	}
	/**
	 * Method that returns columns names and alias of a Dataset that have ignorePersistence to false
	 * @param dataset Dataset from which the columns are obtained
	 * @return
	 */
	def persistentColumns(dataset: Dataset): Map[String, Option[String]] = {
		dataset.schemaDefinition match {
			case SchemaDefinitionTypes.Columns =>
				dataset.schemaColumns.get.columns.filter(col => !col.ignorePersistence.getOrElse(false)).map(col => col.name -> col.alias).toMap
			case SchemaDefinitionTypes.Json =>
				this.structTypeSchema(dataset).filter(col =>
					(col.metadata.contains("ignorePersistence") && !col.metadata.getBoolean("ignorePersistence")) || !col.metadata.contains("ignorePersistence")
				)
					.map(col => {
						val alias: Option[String] = if(col.metadata.contains("alias")) {
							Some(col.metadata.getString("alias"))
						} else {
							None
						}

						col.name -> alias
					}).toMap
			case _ => throw new UnsupportedOperationException("Unsupported schema definition type " + dataset.schemaDefinition.value)
		}
	}

	def isSensitiveField(field: StructField): Boolean = {
		val sensitive = field.metadata.contains("sensitive") && field.metadata.getBoolean("sensitive")

		if(sensitive){
			true
		} else {
			field.dataType match {
				case typ: StructType => {
					for (field <- typ.fields) {
						if (isSensitiveField(field)) {
							return true
						}
					}

					false
				}
				case typ: ArrayType => {
					typ.elementType match {
						case elementType: StructType => {
							for (field <- elementType.fields) {
								if (isSensitiveField(field)) {
									return true
								}
							}

							false
						}
						case elementType: ArrayType => {
							isSensitiveField(StructField("array", elementType, nullable = true))
						}
						case _ =>
							false
					}
				}
				case _ =>
					false
			}
		}
	}

	def sensitiveColumns(dataset: Dataset): List[String] = {
		val schema = this.structTypeSchema(dataset, true)
		val sensitiveFields = new ListBuffer[String]()

		for(field <- schema.fields){
			if(isSensitiveField(field)) {
				sensitiveFields += field.name
			}
		}
		sensitiveFields.toList
	}

	/**
	 * Method that receives a dataType, and returns it, or its internal data types, converted to StringType if it's a sensitive field
	 * @param dataType
	 * @param sensitive
	 * @return
	 */
	def sensitiveFieldsToString(dataType: DataType, sensitive: Boolean = false): DataType = {
		val finalType = dataType match {
			case struct: StructType => {
				var newSchema = struct

				val newFields = newSchema.fields.map(field => {
					val sensitiveField = sensitive || (field.metadata.contains("sensitive") && field.metadata.getBoolean("sensitive"))
					StructField(field.name, sensitiveFieldsToString(field.dataType, sensitiveField), field.nullable, field.metadata)
				})

				newSchema = StructType.apply(newFields)
				newSchema
			}
			case array: ArrayType => {
				val elementType = array.elementType match {
					case struct: StructType => {
						sensitiveFieldsToString(struct, sensitive)
					}
					case array: ArrayType => {
						sensitiveFieldsToString(array, sensitive)
					}
					case typ => {
						if(sensitive)
							StringType
						else
							typ
					}
				}

				ArrayType(elementType)
			}
			case typ => {
				if(sensitive)
					StringType
				else
					typ
			}
		}

		finalType
	}

	/**
	 * Method that receives an StrucType and returns it with all sensitive fields or internal fields converted to StringType
	 * @param schema
	 * @return
	 */
	def getSensitiveSchema(schema: StructType): StructType = {
		var newSchema = schema

		val newFields = newSchema.fields.map(field => {
			val sensitive = field.metadata.contains("sensitive") && field.metadata.getBoolean("sensitive")

			StructField(field.name, sensitiveFieldsToString(field.dataType, sensitive), field.nullable, field.metadata)
		})

		newSchema = StructType.apply(newFields)
		newSchema
	}

	/**
	 * Method that receives a dataset and a column name, and returns the alias of that column, if it exists
	 * @param dataset Dataset containing the column
	 * @param columnName Name of the column to search
	 * @return The alias name of the column, or None if the column doesn't exist or doesn't have an alias
	 */
	def getColumnAlias(dataset: Dataset, columnName: String): Option[String] = {
		dataset.schemaDefinition match {
			case SchemaDefinitionTypes.Columns =>
				dataset.schemaColumns.get.columns.filter(col => col.name.equals(columnName)).map(_.alias).headOption.flatten
			case SchemaDefinitionTypes.Json => {
				this.structTypeSchema(dataset).filter(_.name.equals(columnName)).map(col =>{
					if (col.metadata.contains("alias")) {
						Some(col.metadata.getString("alias"))
					} else {
						None
					}
				}).headOption.flatten
			}
			case _ => throw new UnsupportedOperationException("Unsupported schema definition type " + dataset.schemaDefinition.value)
		}
	}

}
