
name := "spark-windows"

version := "0.1"

scalaVersion := "2.11.12"

val versions = new {
  def spark = "2.3.0"
}

libraryDependencies ++= Seq(
  //spark binaries
  "org.apache.spark" %% "spark-sql" % versions.spark,
  "org.apache.spark" %% "spark-hive" % versions.spark,

  //utils
  "org.clapper" %% "grizzled-slf4j" % "1.0.2",
  "com.typesafe" % "config" % "1.2.1",

  //tests
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${versions.spark}_0.9.0" % "test"
    excludeAll ExclusionRule(organization = "org.mortbay.jetty")
)

assemblyJarName in assembly := "spark-windows.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case x =>
    val baseStrategy = (assemblyMergeStrategy in assembly).value
    baseStrategy(x)
}

// SparkContext is shared between all tests via SharedSingletonContext
parallelExecution in Test := false

test in assembly := {}