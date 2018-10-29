package runnables

import DBSCAN.DBSCAN
import MySparkContext.mySparkContext
import io.InputFileParser
import io.Out._
import org.apache.spark.storage.StorageLevel._


object dbscan {

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
        val hs_outputFile = inputFileParser.getHS_OutputFile()
        val cl_outputFile = inputFileParser.getCL_OutputFile()
        val idCol      = inputFileParser.getID_Col()
        val lonCol     = inputFileParser.getLon_Col()
        val latCol     = inputFileParser.getLat_Col()
        val scoreCol   = inputFileParser.getScore_Col()
        val keyWordCol = inputFileParser.getkeyWord_Col()

        val colSep       = inputFileParser.getCol_Sep()
        val keyWordSep   = inputFileParser.getkeyWord_Sep()
        val userKeywords = inputFileParser.getUserKeyWords()

        val cell_size        = inputFileParser.getCellSize()
        val partition_size_k = inputFileParser.getPartitionSizeK()

        val hs_top_k             = inputFileParser.getHS_Top_k()
        val hs_nb_cell_weight    = inputFileParser.getHS_CellWeight()
        val hs_printAsUnionCells = inputFileParser.getHS_printAsUnionCells()

        //DBSCAN Parameters
        val dbeps  = inputFileParser.getEpsilon()
        val minPts = inputFileParser.getMinPts()

        val dbscan = new DBSCAN()

        //RDD[clusterID, Array[Poi]]
        val dbClusterRDD = dbscan.dbscan(
            inputFile,
            idCol,
            lonCol,
            latCol,
            keyWordCol,
            colSep,
            keyWordSep,
            userKeywords,
            dbeps,
            minPts
        ).persist(MEMORY_AND_DISK)


        //Write clusters to Output.
        writeClusters(dbClusterRDD, cl_outputFile)

        /*
        * Calculate the TotalNumber of Clusters & Total number of Clustered Pois
        * */
        val (totalNumOfClusters, totalNumOfClusteredPois) = dbClusterRDD.aggregate((0, 0))(
            //SeqOp
            (z, x) => {
                (z._1 + 1, z._2 + x._2.size)
            },

            //CombOp
            (z1, z2) => (z1._1 + z2._1, z1._2 + z2._2)

        )

        println("Total #of Clusters = " + totalNumOfClusters)
        println("Total #of Clustered Pois = " + totalNumOfClusteredPois)
        println("Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")


        dbClusterRDD.unpersist()
        dbscan.clear()
        mySparkContext.sc.stop()
    }

}



