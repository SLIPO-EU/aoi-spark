### Overview
This is part of the Slipo project for mining Locations of Interest. It provides distributed implementations in Apache Spark for the following operations:

1. Find hotspots from a collection of Points in 2D space using the Getis-Ord (Gi* statistic).

2. Find clusters implementing a distributed version of DBSCAN.

3. Performs LDA(latent Dirichlet allocation) in a collection of documents.

#### Note

Input coordinates can be transformed on the fly from Source to Destination EPSG codes and back
if specified in config.properties.
For better accuracy you can specify these variables and assign cell-eps(Hotspots) and eps(DBSCAN) in meters. 

### Prerequisites

- sbt (interacive build tool): https://www.scala-sbt.org/download.html

### Usage

How to run Hotspots-Distributed:

1. Download or clone the project.

2. Open terminal inside root folder.

3. sbt package

4. Run spark-submit script as follows:

    ./spark-submit 
    
        --class runnables.(runnable) 
        --master yarn
        --driver-memory 4g 
        --executor-memory 4g
        path-to-generated-jar-file-from-Step-3.jar
        path-to-config.properties-File
        path-to-resources/EPSG_proj.csv

    where --class refers to the main runable class e.g:(hotspots, dbscan or lda).

5. If you want to submit through curl:
   1. Upload somewhere to HDFS the following files:
        1. Generated jar e.g: aoi-spark-2_2.11-0.1.jar
        2. config.properties
        3. resources/EPSG_proj.csv
        4. lib dir.
   
   2. Fill config and app.json with appropriate variables.
      (app.json takes 2 arguments in args: path_to_config, path_to_EPSG_proj)
   
   3. Run by submitting job to Spark cluster e.g: Yarn by:
         curl -d @app.json -H 'Content-Type: application/json' -X POST ..Cluster_Path/batches

Notice: aoi-spark is build under Spark Version 2.2.3.
It is recommended for the Spark cluster to have the same version.

### License

The contents of this project are licensed under the [Apache License 2.0](https://github.com/SLIPO-EU/loci/blob/master/LICENSE).
