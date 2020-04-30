name := "turf_tuc_ingestion"

organization := "it.csi"

version := "1.0"

scalaVersion := "2.10.5"

autoScalaLibrary := false


resolvers += "Repart" at "", [organization]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]
libraryDependencies += "oracle.ojdbc" % "ojdbc-14" % "1.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.4"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
