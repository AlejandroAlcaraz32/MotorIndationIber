package com.minsait.metagold.activity

import com.minsait.metagold.activity.statistics.models.ActivityResults.{ActivityResult, Fail, Success}
import com.minsait.metagold.activity.statistics.models.ActivityTriggerTypes.Adf
import com.minsait.metagold.activity.statistics.models.{ActivityDuration, ActivityExecution, ActivityParameter, ActivityResults, ActivityStatistics, ActivityTransformation, ActivityTransformationStage, ActivityTrigger, GoldValidator}
import com.minsait.common.configuration.ConfigurationReader
import com.minsait.common.configuration.models.IndationProperties
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.metagold.logging.Logging
import com.minsait.metagold.metadata.models.Activity
import com.minsait.metagold.metadata.MetadataFilesManager
import com.minsait.metagold.metadata.models.enums.ActivityTypes
import com.minsait.common.spark.SparkSessionWrapper
import com.minsait.metagold.metadata.models.enums.TransformationTypes.{Datalake, SQL}
import io.jvm.uuid.StaticUUID
import org.apache.commons.cli.{BasicParser, CommandLine, Options}

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ActivityProcess(override val indationProperties: IndationProperties
                      , parameters: CommandLine)
  extends SparkSessionWrapper with ConfigurationReader with Logging {

  private val metadataManager: MetadataFilesManager = new MetadataFilesManager(indationProperties)

  //TODO: Gestión completa de registro de estadísticas de carga

  /**
   * Ejecuta todas las transformaciones especificadas en un json de actividades de transformación
   *
   * @param activityId : Name of the transformation activity to be executed
   * @param triggerId  : Identifier of the Data Factory pipeline which executed this activity
   * @param args       : Parameters or arguments received in command line.
   */
  def execute(
               activityId: String,
               triggerId: String,
               args: Array[String]
             ): ActivityStatistics = {

    val startTimeMillis = System.currentTimeMillis()
    val uuidActivity = StaticUUID.randomString
    this.logger.info(s"Starting activity: $activityId with UUID $uuidActivity")

    // start statistics
    var activityStatistics: ActivityStatistics = {
      new ActivityStatistics(
        uuid = uuidActivity,
        trigger = ActivityTrigger(Adf, triggerId),
        activity = ActivityExecution(activityId, parameters.getOptions.map(p => ActivityParameter(p.getLongOpt, p.getValue)).toList),
        activityDuration = ActivityDuration(new Timestamp(startTimeMillis), new Timestamp(startTimeMillis), 0)
      )
    }

    try {

      val metadataReader = new MetadataFilesManager(indationProperties)

      //obtener json de actividades de transformación por nombre
      val activity = 
            if (activityId.endsWith(".json")) { metadataReader.activityByPath(activityId) }
            else {
              try {
                logger.info("Attempting to load activity from constructed path: " + activityId)
                // Attempt to construct the path from activityId
                val extractedSourceName = activityId.split("-")(0) // Extracting the SourceName from activityId
                val constructedPath = s"activities/$extractedSourceName/activity-$activityId.json"
                logger.info("Constructed Path: " + constructedPath)
                // Try to load the dataset using the constructed path
                metadataReader.activityByPath(constructedPath)
              } catch {
                case _: Exception => 
                  // If it fails, fall back to activityByName
                  logger.info("Could not find the activity in constructed path. Searching by name...")
                  metadataReader.activityByName(activityId)
              }
            }

      if (activity.isEmpty) {
        val t = new Timestamp(startTimeMillis)
        logger.error(s"Activity not defined: $activityId", None)
        activityStatistics = activityStatistics.copy(
          resultMsg = s"Activity not defined: $activityId", // mensaje
          // incluimos una transformación dummy para evitar la ruptura del esquema de estadísticas
          transformations = List(ActivityTransformation("dummy", "dummy", ActivityResults.Fail, ActivityDuration(t, t, 0), List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(t, t, 0))), "dummy",List("")))
        )
      }
      else {
        //Recorrer transformaciones de fichero y ejecutar cada transformacion
        val transformProcess = new TransformProcess(indationProperties, triggerId, parameters, uuidActivity)

        // Ejecución ordenada clásica
        if (activity.get.activityType.isEmpty || activity.get.activityType.get == ActivityTypes.Concurrent) {
          activity.get.transformations.get.foreach(transformName => {

            // Se continúa con el bucle mientras no haya tenido error la transformación anterior
            if (activityStatistics.resultMsg == "") {
              val transform = 
                if (transformName.endsWith(".json")) 
                  metadataReader.transformationByPath(transformName) 
                else {
                  try {
                    logger.info("Attempting to load transformation from constructed path: " + transformName)
                    // Attempt to construct the path from activityId and transformName
                    val extractedSourceName = activityId.split("-")(0)
                    val constructedPath = s"transformations/$extractedSourceName/transform-$transformName.json"
                    logger.info("Constructed Path: " + constructedPath)
                    // Try to load the dataset using the constructed path
                    metadataReader.transformationByPath(constructedPath)
                  } catch {
                    case _: Exception => 
                      // If it fails, fall back to transformationByName
                      logger.info("Could not find the transformation in constructed path. Searching by name...")
                      metadataReader.transformationByName(transformName)
                  }
                }
                  
              if (transform.isEmpty) {
                val msg = s"Transformation $transformName not defined"
                val endTimeMillis = System.currentTimeMillis()
                val t = new Timestamp(startTimeMillis)
                val transformStatistics = ActivityTransformation(transformName, "unknown", Fail, ActivityDuration(new Timestamp(endTimeMillis), new Timestamp(endTimeMillis), 0), List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(t, t, 0))), msg,List(""))
                logger.error(msg, None)
                // hay que parar el bucle
                activityStatistics = activityStatistics.copy(
                  transformations = activityStatistics.transformations ++ List(transformStatistics),
                  resultMsg = msg,
                  activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
                )
              }
              else {

                // Execute transformation and add result to statistics
                val transformationResult = transformProcess.execute(transform.get, metadataManager)
                activityStatistics = activityStatistics.copy(transformations = activityStatistics.transformations ++ List(transformationResult))

                // If tansformation failed, log error and return results
                if (transformationResult.transformationResult == Fail) {
                  val msg = s"Activity ${activity.get.name} failed in transformation ${transformName}"
                  val endTimeMillis = System.currentTimeMillis()
                  logger.error(msg, None)
                  // hay que parar el bucle
                  activityStatistics = activityStatistics.copy(
                    resultMsg = msg,
                    activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
                  )
                }

/*                if (transformationResult.transformationResult == Success & transform.get.qualityRules.isDefined) {
                  transform.get.typ match {
                    case Datalake =>
                      val table = transform.get.datalakeTransformation.get.table
                      val database = transform.get.datalakeTransformation.get.database
                      val dataframe = spark.sql("select * from " + table + "." + database)
                      GoldValidator.validateQuality(transform.get, dataframe)

                    case SQL =>
                      val tables = transform.get.sqlTransformation.get.sourceTables
                  }
                }*/

              }
            }
          }
          )
          // En caso de llegar aquí, es que la ejecución ha ido bien registramos el éxito
          if (activityStatistics.resultMsg == "") {
            logger.info(s"Activity ${activity.get.name} executed successfully.")
            val endTimeMillis = System.currentTimeMillis()
            activityStatistics = activityStatistics.copy(
              activityResult = Success,
              activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
            )

          }

        }
        // Ejecución paralela basada en dependencias entre transformaciones
        // La ejecución se llevará a cabo en un pool de Futuros. Cada transformación realizará
        else if (activity.get.activityType.get == ActivityTypes.Parallel) {

          //  Debemos validar que no hay dependencias de transformaciones inexistentes ni dependencias circulares
          val validationReesult = activity.get.validateDependencies
          if (!validationReesult._1) {
            // Error de validación de las dependencias de la actividad
            val msg = s"Activity dependencies validation failed for activity ${activity.get.name}: ${validationReesult._2}"
            val endTimeMillis = System.currentTimeMillis()
            val t = new Timestamp(startTimeMillis)
            logger.error(msg, None)
            activityStatistics = activityStatistics.copy(
              resultMsg = msg,
              // incluimos una transformación dummy para evitar la ruptura del esquema de estadísticas
              transformations = List(ActivityTransformation("dummy", "dummy", ActivityResults.Fail, ActivityDuration(t, t, 0), List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(t, t, 0))), "dummy",List())),
              activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
            )
            // paramos ejecución devolviendo el resultado
            return activityStatistics
          }
          // La validación de dependencias ha tenido éxito
          else {

            // Inicializamos resultado de actividad a true
            var activitySuccess = true

            // Definimos pool de futuros
            var futures: Map[String, Future[ActivityTransformation]] = Map()

            // Recorremos actividades e inicializamos la ejecución de sus futuros
            activity.get.parallelTransformations.get.foreach(t => {
              // Creamos el nuevo futuro
              val f = Future[ActivityTransformation]({

                // Cada futuro debe esperar primero a la ejecución de sus dependencias
                var depencenciesSuccess = true
                t.dependencies.foreach(d => {

                  // primero esperamos a que estén añadidas las dependencias, ya que pueden estar aún creándose todos los futuros
                  while (!futures.contains(d)) {
                    Thread.sleep(100)
                  }

                  // obtenemos resultado de las dependencias
                  val result: ActivityTransformation = Await.result(futures(d), Duration.Inf)

                  // si alguna ha fallado, bloqueamos ejecución
                  if (result.resultMsg != "")
                    depencenciesSuccess = false
                })

                // Sólo si las dependencias han tenido éxito, continuamos con la ejecución
                if (depencenciesSuccess) {

                  val tname = t.name
                  val transform = 
                    if (tname.endsWith(".json")) 
                      metadataReader.transformationByPath(tname) 
                    else {
                      try {
                        logger.info("Attempting to load activity from constructed path: " + tname)
                        // Attempt to construct the path from activityId and tname
                        val extractedSourceName = activityId.split("-")(0)
                        val constructedPath = s"transformations/$extractedSourceName/transform-$tname.json"
                        logger.info("Constructed Path: " + constructedPath)
                        // Try to load the dataset using the constructed path
                        metadataReader.transformationByPath(constructedPath)
                      } catch {
                        case _: Exception => 
                          // If it fails, fall back to transformationByName
                          logger.info("Could not find the transformation in constructed path. Searching by name...")
                          metadataReader.transformationByName(tname)
                      }
                    }

                  if (transform.isEmpty) {
                    val msg = s"Transformation ${t.name} not defined"
                    val endTimeMillis = System.currentTimeMillis()
                    val time = new Timestamp(startTimeMillis)
                    val transformStatistics = ActivityTransformation(t.name, "unknown", Fail, ActivityDuration(new Timestamp(endTimeMillis), new Timestamp(endTimeMillis), 0), List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(time, time, 0))), msg,List(""))
                    logger.error(msg, None)

                    activityStatistics = activityStatistics.copy(
                      transformations = activityStatistics.transformations ++ List(transformStatistics),
                      resultMsg = msg,
                      activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
                    )
                    activitySuccess = false
                    // devolvemos resultado
                    transformStatistics
                  }
                  else {

                    // Execute transformation and add result to statistics
                    val transformationResult = transformProcess.execute(transform.get, metadataManager)
                    activityStatistics = activityStatistics.copy(transformations = activityStatistics.transformations ++ List(transformationResult))

                    // If tansformation failed, log error and return results
                    if (transformationResult.transformationResult == Fail) {
                      val msg = s"Activity ${activity.get.name} failed in transformation ${t.name}"
                      val endTimeMillis = System.currentTimeMillis()
                      logger.error(msg, None)
                      activityStatistics = activityStatistics.copy(
                        resultMsg = msg,
                        activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
                      )
                      activitySuccess = false
                    }

                    // devolvemos resultado de transformación
                    transformationResult
                  }

                } else {
                  // En caso contrario, generamos error en la salida
                  val msg = s"Error executing dependencies of Transformation ${t.name}. Transformation cancelled."
                  val endTimeMillis = System.currentTimeMillis()
                  val time = new Timestamp(startTimeMillis)
                  val transformStatistics = ActivityTransformation(t.name, "unknown", Fail, ActivityDuration(new Timestamp(endTimeMillis), new Timestamp(endTimeMillis), 0), List(ActivityTransformationStage("dummy", "dummy", ActivityResults.Fail, 0, ActivityDuration(time, time, 0))), msg,List(""))
                  logger.error(msg, None)
                  activityStatistics = activityStatistics.copy(
                    transformations = activityStatistics.transformations ++ List(transformStatistics),
                    resultMsg = msg,
                    activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
                  )
                  activitySuccess = false
                  // devolvemos resultado de la transformación
                  transformStatistics
                }

              })

              // Lo añadimos al pool
              futures = futures ++ Map(t.name -> f)

            })

            // Por último, analizamos todos los resultados para definir si la actividad ha tenido éxito
            futures.values.foreach(f => {
              // Esperamos al resultado de ejecución nuevamente. TODO: VALIDAR SI SE PUEDE ESPERAR DOS VECES EL RESULTADO DE UN FUTURO
              val result: ActivityTransformation =
                if (!f.isCompleted)
                  Await.result(f, Duration.Inf)
                else
                  f.result(Duration.Inf)(null)
              if (result.transformationResult == ActivityResults.Fail)
                activitySuccess = false

            })

            // En caso de éxito de actividad, registramos el éxito
            if (activitySuccess) {
              logger.info(s"Activity ${activity.get.name} executed successfully.")
              val endTimeMillis = System.currentTimeMillis()
              activityStatistics = activityStatistics.copy(
                activityResult = Success,
                activityDuration = ActivityDuration(activityStatistics.activityDuration.start, new Timestamp(endTimeMillis), endTimeMillis - startTimeMillis)
              )
            }
          }
        }
        else {
          throw new UnsupportedOperationException(s"Activity type ${activity.get.activityType.get.value} not supported in activity ${activity.get.name}")
        }

      }

      // devolvemos el resultado
      activityStatistics
    }
    catch {
      case ex: Throwable => {
        logger.error(s"Unexpected error raised in activity $activityId.", Some(ex))
        // registramos estadísticas generales con el error
        activityStatistics.copy(resultMsg = s"Unexpected error raised in activity $activityId.\n${ex.getMessage}")
      }
    }

  }

}
