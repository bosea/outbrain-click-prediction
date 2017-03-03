name := "outbrain-model"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.1"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

libraryDependencies += "com.google.guava" % "guava" % "12.0.1"


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "glassfish", impl @ _*) => MergeStrategy.last
  case PathList("org.glassfish.hk2.external", "aopalliance-repackaged", impl @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", impl @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("com", "databricks", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
