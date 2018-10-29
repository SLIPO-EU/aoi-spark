name := "slipo-spark"

version := "0.1"


scalaVersion := "2.11.12"


libraryDependencies ++= {

    val sparkVer = "2.3.0"

    Seq(
        "org.apache.spark" %% "spark-core" % sparkVer,
        "org.apache.spark" %% "spark-sql" % sparkVer,

        "org.datasyslab" % "geospark" % "1.1.3",
        "org.datasyslab" % "geospark-sql_2.3" % "1.1.3"
    )
}


assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}

