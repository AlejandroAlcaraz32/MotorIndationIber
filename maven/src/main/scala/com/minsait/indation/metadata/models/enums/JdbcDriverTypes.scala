package com.minsait.indation.metadata.models.enums

object JdbcDriverTypes {
	sealed case class JdbcDriverType(value: String)
	object Oracle extends JdbcDriverType("oracle.jdbc.driver.OracleDriver")
	object MSSql extends JdbcDriverType("com.microsoft.sqlserver.jdbc.SQLServerDriver")
	object H2 extends  JdbcDriverType("org.h2.Driver")
	object MySQL extends  JdbcDriverType("org.mariadb.jdbc.Driver")
	object SAP extends  JdbcDriverType("com.sap.db.jdbc.Driver")
}
