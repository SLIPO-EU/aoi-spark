package runnables

import DBPOI.DBPOI
import DBSCAN.DBSCAN
import io.{InputFileParser, Spatial}
import mySparkSession.mySparkSession
import io.Out


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
        val otherCols  = inputFileParser.getOtherCols()

        val colMap     = inputFileParser.getColMap()

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

        //EPSG
        val source_crs = inputFileParser.getSourceCrs()
        val target_crs = inputFileParser.getTargetCrs()

        val spatial = Spatial()
        val poiRDD  = spatial.getLonLatRDD(
            inputFile,
            idCol,
            lonCol,
            latCol,
            scoreCol,
            keyWordCol,
            userKeywords,
            colMap,
            otherCols,
            colSep,
            keyWordSep,
            source_crs,
            target_crs
        )


        val dbscan = new DBSCAN()
        val finalRDD = dbscan.dbscan(pointRDD = poiRDD, eps = dbeps, minPts = minPts)

        /*
        finalRDD.take(20).foreach{
            t => {
                var s = s"(${t._1})(${t._2._1._1}, ${t._2._1._2}):"

                var s_map = ""
                t._2._1._3.foreach{
                    case (k, v) => {
                        if(v.isInstanceOf[Array[String]]){
                            s_map += (k + " --> " + v.asInstanceOf[Array[String]].mkString(",")) + ", "
                        }
                        else{
                            s_map += k + " --> " + v.toString + ", "
                        }
                    }
                }

                println(s"$s ($s_map)(${t._2._2})")
            }
        }

        println()
        println("Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")
        */


        val groupedRDD = finalRDD.map{
            case (id, ((lon, lat, hm), cID)) => {
                (cID, DBPOI(id, lon, lat))
            }
        }.aggregateByKey(Array[DBPOI]())(_ :+ _, _ ++ _)


        //Write clusters to Output.
        Out.writeClusters(groupedRDD, cl_outputFile)

        val (numOfClusters, numOfClusteredPois) = groupedRDD.aggregate(0, 0)(
            (z, x) => (z._1 + 1, z._2 + x._2.size),
            (z1, z2) => (z1._1 + z2._1, z1._2 + z2._2)
        )

        println(s"NumOfClusters = $numOfClusters")
        println(s"NumOfClusteredPois = $numOfClusteredPois")

        println("Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        dbscan.clear()
        mySparkSession.spark_session.stop()
    }

}

