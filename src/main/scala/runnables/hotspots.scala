package runnables

import HotSpots._
import io.{InputFileParser, Spatial, Out}
import mySparkSession.mySparkSession

object hotspots {

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


        val hotSpots = new Hotspots()

        val hotSpotsArr = hotSpots.hotSpots(
            poiRDD,
            scoreCol,
            cell_size,
            partition_size_k,
            hs_top_k,
            hs_nb_cell_weight,
            hs_printAsUnionCells
        )

        Out.write_hotspots(hotSpotsArr, hs_outputFile, delimiter = ";" )

        println("Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        mySparkSession.spark_session.stop()
    }
    
}
