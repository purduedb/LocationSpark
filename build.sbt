name := "SpatialSpark"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "1.5.1" )

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"

libraryDependencies +="com.google.uzaygezen" % "uzaygezen-core" % "0.2"

libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
