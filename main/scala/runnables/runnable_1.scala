package runnables

import java.util

import DBSCAN.DBSCAN_Clusterer
import MySparkContext.mySparkContext
import com.vividsolutions.jts.geom.GeometryFactory
import gr.athenarc.imsi.slipo.analytics.POI
import io.InputFileParser

import org.apache.spark.storage.StorageLevel._



object runnable_1 {

    def main(args: Array[String]): Unit = {


        if(args.size != 1){
            println("You should give the properties FilePath as argument to main...")
            return
        }


        val startTime = System.nanoTime()



        val inputFileParser = new InputFileParser(args(0))


        if(!inputFileParser.loadPropertiesFile()){
            return
        }



        val inputFile  = inputFileParser.getInputFile()
        val outputFile = inputFileParser.getOutputFile()


        val idCol      = inputFileParser.getID_Col()
        val lonCol     = inputFileParser.getLon_Col()
        val latCol     = inputFileParser.getLat_Col()


        val colSep       = inputFileParser.getColSep()

        val minLon = inputFileParser.getMinLon()
        val maxLon = inputFileParser.getMaxLon()

        val minLat = inputFileParser.getMinLat()
        val maxLat = inputFileParser.getMaxLat()



        val epsilon = inputFileParser.getDBSCAN_epsilon()
        val minPts  = inputFileParser.getDBSCAN_minPts()


        val gridPS_Size = inputFileParser.getGridPartitionSize()
        val lambda = inputFileParser.getLambda()

        val geometryFactory = new GeometryFactory()


        val dBSCAN_Clusterer = new DBSCAN_Clusterer()


        //RDD[poiID, lon, lat]
        val poiRDD = mySparkContext.sc.textFile(inputFile)
                                      .map{
                                          line => {

                                              try {

                                                  val parts = line.split(colSep)

                                                  (parts(idCol), parts(lonCol).toDouble, parts(latCol).toDouble)

                                              }
                                              catch {
                                                  case e: Exception => ("N/A", 0.0, 0.0)
                                              }
                                          }
                                      }
                                      .filter(x => x._1 != "N/A")


        //RDD[(clusterID, Set[poiID])]
        val clusterRDD = dBSCAN_Clusterer.dbscan(poiRDD, epsilon, lambda, minPts, minLon, maxLon, minLat, maxLat, geometryFactory)
                                         .persist(MEMORY_AND_DISK_SER)



        val (totalNumOfClusters, totalNumOfClusteredPois) = clusterRDD.aggregate((0, 0))(
            //SeqOp
            (z, x) => {
                (z._1 + 1, z._2 + x._2.size)
            },

            //CombOp
            (z1, z2) => (z1._1 + z2._1, z1._2 + z2._2)

        )


        println("Keywords-Free")
        println("epsilon = " + epsilon)
        println("minPts  = " + minPts)
        println("\nTotal #of Clusters = " + totalNumOfClusters)
        println("Total #of Clustered Pois = " + totalNumOfClusteredPois)



        println("Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")


        clusterRDD.unpersist()
        dBSCAN_Clusterer.clean_BD_VArs()

        mySparkContext.sc.stop()

    }

}




