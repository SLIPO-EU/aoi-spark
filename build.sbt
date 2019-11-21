name := "aoi-spark-2"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= {

    val sparkVer = "2.2.3"

    Seq(
        "org.apache.spark" %% "spark-core" % sparkVer,
        "org.apache.spark" %% "spark-sql" % sparkVer
    )
}


