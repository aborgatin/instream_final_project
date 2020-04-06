
name := "spark-streaming-fraud-detector"

version := "0.1"

scalaVersion := "2.11.12"

sparkVersion := "2.4.3"

sparkComponents ++= Seq("sql", "streaming")

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"

libraryDependencies += "org.apache.ignite" % "ignite-spark-2.4" % "2.8.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"




assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.first
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
