def start() {
	log.info "COMPILE"
	
	String dir = "${WORKSPACE}"
	String prj = "${PRJ}"
	String version = "${VERSION}"
	zipper.packing( dir , prj , version )
	
	return 0; // devolveremos como error la fase de compilacion para que no continue el pipe
}

return this;
