name := "Slipo_Gen"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= {

    val sparkVer = "2.3.0"
    val geosparkVer = "1.2.0"

    Seq(
        "org.apache.spark" %% "spark-core" % sparkVer,
        "org.apache.spark" %% "spark-sql" % sparkVer,
        "org.apache.spark" %% "spark-mllib" % sparkVer,

        "org.datasyslab" % "geospark" % geosparkVer,
        "org.datasyslab" % "geospark-sql_2.3" % geosparkVer,

        "org.osgeo" % "proj4j" % "0.1.0"
    )
}

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}


