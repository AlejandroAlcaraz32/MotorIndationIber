package com.minsait.indation.metadata

import com.minsait.indation.metadata.exceptions.NonUniqueDatasetException
import com.minsait.indation.metadata.models.{Dataset, Source}
import org.joda.time.{DateTime, DateTimeZone}

trait MetadataReader {
  val sources: List[Source]
  val datasets: List[Dataset]

  def datasetForFile(fileName: String): Option[Dataset] = {
    val matchedDatasets = datasets.filter( p = dataset =>
      dataset.isActive(DateTime.now(DateTimeZone.UTC).getMillis)
        && dataset.matchesFileName(fileName)
    )

    if(matchedDatasets.groupBy(_.name).keys.toList.length > 1) throw new NonUniqueDatasetException("Non unique dataset for file: " + fileName)

   matchedDatasets.sortWith((a,b) => a.effectiveDate.get.compareTo(b.effectiveDate.get) < 1).reverse.headOption
  }

  def datasetByName(name: String): Option[Dataset] = {

    val matchedDatasets = datasets.filter( p = dataset =>
      dataset.isActive(DateTime.now(DateTimeZone.UTC).getMillis)
        && dataset.name.equals(name)
    )

    if(matchedDatasets.groupBy(_.name).keys.toList.length > 1) throw new NonUniqueDatasetException("Non unique dataset for name: " + name)

    matchedDatasets.sortWith((a,b) => a.effectiveDate.get.compareTo(b.effectiveDate.get) < 1).reverse.headOption
  }

  // Devuelve todos los datasets activos que apuntan a un origen específico.
  def datasetBySource(source: Source): List[Dataset]={

    // En matchedDatasets estarán todos los datasets activos, ordenados por fecha de validez descendente
    val matchedDatasets = datasets.filter( p = dataset =>
      dataset.isActive(DateTime.now(DateTimeZone.UTC).getMillis)
        && dataset.sourceName.equals(source.name)
    )

    // Hay que devolverlos agrupados por nombre y quedándonos con el de mayor fecha
    val mapDatasets = matchedDatasets.groupBy(_.name)
    mapDatasets.map(x => x._2.sortWith((a,b) => a.effectiveDate.get.compareTo(b.effectiveDate.get) < 1).reverse.head).toList
  }

  def sourceByName(name: String): Option[Source] = {
    sources.filter(_.name.equals(name)).headOption
  }
}
