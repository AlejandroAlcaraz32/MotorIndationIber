package com.minsait.indation.metadata.models

case class ApiInput(
                     endpoint: String,
                     parameters: Option[String],
                     pagination: Pagination,
                     dataIn: Option[String],
                     headers: Option[Map[String, String]]

)
