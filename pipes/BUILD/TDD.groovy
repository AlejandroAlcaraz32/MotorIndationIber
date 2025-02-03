def start() {
   log.info "Starting Sonar scan"

   def scannerHome = tool 'SCANNER4'
	def project = PRJ.replace('/', '_')
	def VERSION = env.VERSION

   withSonarQubeEnv('SONAR_ACCIONA') {

      //artifactory.mavenBuildDepoy("corporativo-maven", "corporativo-maven-release", "pom.xml", "test")

      //String cmd2Run = "${scannerHome}/bin/sonar-scanner -Dsonar.language=java -Dsonar.java.binaries=target/classes -Dsonar.sources=src -Dsonar.projectKey=${project} -Dsonar.projectVersion=${VERSION}"
	   //status = sonar.runInSonarQubeEnv(cmd2Run, 10, project)
   }

   return 0;
}

return this;