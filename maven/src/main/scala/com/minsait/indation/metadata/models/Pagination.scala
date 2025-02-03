package com.minsait.indation.metadata.models

import com.minsait.indation.metadata.models.enums.PaginationTypes.PaginationType

case class Pagination(
                       typ: PaginationType,
                       top: Option[String],
                       skip: Option[String],
                       topSize: Option[Int],
                       nextUrl: Option[String]
                         )
