// Funcion de arranque del proceso de despliegue de desarrollo
// devuelve 0 si OK  y <>0 si ERROR
def start(pathVersion, def pathConf="") {
	
	try
	{
		println "FASE DOCKER"
		args = "--build-arg JAR_PATH=${pathVersion}"
		//Posibles funciones a realizar están en la clase vars/container.groovy
		//son: buildImage(args), pushImage(image, tag) y buildAndPushImage(args)

		//container.buildAndPushImageWithCache(args)
	}
	catch(Exception e) 
	{
	   echo "Error " + e.toString()
	   throw e
	}
	return 0; 
}

return this;