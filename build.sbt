name := """spark"""

scalaVersion := "2.10.3"

resolvers +=
  "nlpcn" at "http://maven.nlpcn.org"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0"
libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "6.1.0"
