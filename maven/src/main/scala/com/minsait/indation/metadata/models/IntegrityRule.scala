package com.minsait.indation.metadata.models

case class IntegrityRule(name: String,
                         localColumns: List[String],
                         referencedTable: String,
                         referencedColumns: List[String])

