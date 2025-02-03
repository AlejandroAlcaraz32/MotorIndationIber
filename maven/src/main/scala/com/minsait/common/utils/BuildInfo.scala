package com.minsait.common.utils

// TODO: generar código bueno para extraer la versión del jar y el nombre a partir de la info del pom.
object BuildInfo {
  val name =  if ( this.getClass().getPackage().getName() == null) "" else this.getClass().getPackage().getName()
  val version = if ( this.getClass().getPackage().getSpecificationVersion() == null) "" else this.getClass().getPackage().getSpecificationVersion()

}
