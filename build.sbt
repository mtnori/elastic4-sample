name := "elastic4s-sample"

version := "0.1"

scalaVersion := "2.13.7"

val elastic4sVersion = "7.16.0"

libraryDependencies ++= Seq(
  // recommended client for beginners
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
  // test kit
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test"
)
