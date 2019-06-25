### Overview
This is part of the Slipo project for mining Locations of Interest. It provides distributed implementations in Apache Spark for the following operations:

1. Find hotspots from a collection of Points in 2D space using the Getis-Ord (Gi* statistic).

2. Find clusters implementing a distributed version of DBSCAN.

3. Performs LDA(latent Dirichlet allocation) in a collection of documents.

### Prerequisites

- sbt (interacive build tool): https://www.scala-sbt.org/download.html
 - Spark: http://spark.apache.org/downloads.html

### Usage

How to run Hotspots-Distributed:

1. Download or clone the project.

2. Open terminal inside root folder.

3. type: sbt clean assembly to generate the jar file.

4. Go to the installation folder of spark (e.g., cd spark-version.../bin).

5. Run spark-submit script as follows:
     ./spark-submit --class runnables.hotspots --master local[*]  --driver-memory 4g --executor-memory 4g  path-to-generated-jar-file-from-Step-3.jar  path-to-config.properties-File

     ./spark-submit --class runnables.dbscan   --master local[*]  --driver-memory 4g --executor-memory 4g  path-to-generated-jar-file-from-Step-3.jar  path-to-config.properties-File

     ./spark-submit --class runnables.lda   --master local[*]  --driver-memory 4g --executor-memory 4g  path-to-generated-jar-file-from-Step-3.jar  path-to-config.properties-File
 
    where --class refers to the main class (runnables.hotspots).

### License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/SLIPO-EU/loci/blob/master/LICENSE).
