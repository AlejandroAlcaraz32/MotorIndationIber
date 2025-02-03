package com.minsait.indation.metadata.models

case class TableInput(
                       table: String,
                       schema: Option[String],
                       query: Option[String],
                     //TODO: No llevaría a indation la lógica asociada a partition column + numpartitions.
                       partitionColumn: Option[String],
//                       lowerBound: Option[String], // de momento lo obtenemos automático
//                       upperBound: Option[String],// de momento lo obtenemos automático
                       numPartitions: Option[String],
                       fetchsize: Option[String],
                       //En caso de incremental o last_changes, para traer únicamente del origen los registros nuevos
                       //respecto a los datos que ya hay en silver
                       onlyNewData: Option[Boolean],
                       //Para establecer el patrón para poder convertir el campo isTimestamp a string en caso de tratarse
                       //de un campo date o datetime, para realizar el filtro de onlyNewData en la tabla del origen de datos
                       datePattern: Option[String]
                     ){

  def getTable:String ={
    if (query.isDefined)
      s"(${query.get}) $table" // Si se define consulta, se devuelve ésta formateada para que el conector sepa leerla
    else if(schema.isDefined)
      s"${schema.get}.${table}"
    else table
  }

  def getTableNoQuery: String = {
    if (schema.isDefined)
      s"${schema.get}.${table}"
    else table
  }

  def getTablePath:String ={
    if (query.isDefined)
      s"$table.personalizedQuery" // Si se define consulta, se devuelve ésta formateada para que el conector sepa leerla
    else if(schema.isDefined)
      s"${schema.get}.${table}"
    else table
  }

}
