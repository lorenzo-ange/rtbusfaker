name := "RtBusFaker"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "INDEX.LIST", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties", xs @ _*) => MergeStrategy.first
  case _ => MergeStrategy.deduplicate
}