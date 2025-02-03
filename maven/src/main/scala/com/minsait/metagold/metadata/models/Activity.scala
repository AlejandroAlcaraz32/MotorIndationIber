package com.minsait.metagold.metadata.models

import com.minsait.metagold.metadata.models.enums.ActivityTypes
import com.minsait.metagold.metadata.models.enums.ActivityTypes.ActivityType

case class Activity(
                     name: String,
                     description: String,
                     activityType: Option[ActivityType],
                     transformations: Option[ List[String]],
                     parallelTransformations: Option[List[TransformationExecution]],
                     parameters: Option[List[Parameter]]
                   ){

  def validateDependencies:(Boolean,String)={
    try {
      if (activityType.get == ActivityTypes.Parallel && parallelTransformations.isDefined) {
        parallelTransformations.get.foreach(t => {
          var dependencies: List[String] = List()
          var newDependencies = getDependencies(t.name, "<root>")
          while (newDependencies.length > 0) {

            // Si las nuevas dependencias repiten alguna de las ya existentes, hay una dependencia circular
            newDependencies.map(d => {
              if (dependencies.contains(d)) {
                throw new UnsupportedOperationException(s"Parallel transformation ${t.name} contains circular dependencies: $d")
              }
            })

            // Si no hay fallos de validación, agregamos las dependencias a la lista
            dependencies = dependencies ++ newDependencies

            // Recalculamos las nuevas dependencias
            newDependencies = newDependencies.map(d => getDependencies(d, t.name)).flatten
          }
        })
        (true, "")
      }
      else (true, "") // no hay dependencias que revisar
    }
    catch{
      case ex: Throwable =>{
        (false,ex.getMessage)
      }
    }
  }

  def getDependencies(transformationName: String, parentTransformation: String):List[String]={
    val transform = parallelTransformations.get.filter(t => t.name==transformationName)

    // Si no se encuentra la dependencia, hay error de validación por depender de una transformación inexistente
    if (transform.isEmpty){
      throw new UnsupportedOperationException(s"Parallel transformation $parentTransformation depends on non existing transformation $transformationName")
    }
    // Si se repite varias veces la misma transformación, hay error de validación por no estar permitido ejecutar dos veces la misma transformación
    else if (transform.length>1){
      throw new UnsupportedOperationException(s"Activity has duplicated parallel transformations defined $transformationName")
    }
    else{
      transform(0).dependencies
    }
  }
}