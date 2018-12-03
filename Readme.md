
#############################################################################################
This is part of the Slipo project for mining Locations of Interest 
This is the distributed version for finding hotspots in Apache Spark & Scala.

Authors: 

Panagiotis Kalampokis

#############################################################################################

Prerequisites:

1) sbt   (interacive build tool): 
https://www.scala-sbt.org/download.html

2) Spark
http://spark.apache.org/downloads.html


How to Run Hotspots-Distributed:

1) Download or clone the project.

2) Open terminal inside root folder.

3) type: sbt clean assembly to generate the jar file.

4) Go to the installation folder of spark: 

     cd spark-version.../bin

5)   run spark-submit script as follows:
     ./spark-submit --class runnables.hotspots --master local[*]  --driver-memory 4g --executor-memory 4g  path-to-generated-jar-file-from-Step-3.jar  path-to-config.properties-File
     ./spark-submit --class runnables.dbscan   --master local[*]  --driver-memory 4g --executor-memory 4g  path-to-generated-jar-file-from-Step-3.jar  path-to-config.properties-File 



--class is our main class:
Here is runnables.hotspots

--master local[*] If you run it in local mode. 
Otherwise, check Spark documentation for cluster mode (e.g, Mesos or Yarn).
