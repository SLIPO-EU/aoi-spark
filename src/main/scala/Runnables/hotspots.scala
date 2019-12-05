package Runnables

import HotSpots._
import io.{InputFileParser, Spatial}
import mySparkSession.mySparkSession
import scala.collection.mutable.ArrayBuffer

object hotspots {

    def main(args: Array[String]): Unit = {

        val logArrBuff: ArrayBuffer[String] = ArrayBuffer[String]()
        val startTime = System.nanoTime()

        if(args.size != 2){
            logArrBuff += "You should give the properties FilePath, and EPSG_ filePath as argument to main..."
            println("You should give the properties FilePath, and EPSG_ filePath as argument to main...")
        }

        val inputFileParser = new InputFileParser(args(0))
        val (finalCheck, inputLog) = inputFileParser.loadPropertiesFile()

        logArrBuff ++= inputLog

        if(!finalCheck){
            return
        }

        val inputFile  = inputFileParser.getInputFile()
        val hs_outputFile = inputFileParser.getHS_OutputFile()
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

        //EPSG
        val source_crs = inputFileParser.getSourceCrs()
        val target_crs = inputFileParser.getTargetCrs()

        //Log Path
        val outLogPath = hs_outputFile + "/Log"

        val spatial = Spatial()
        val (poiRDD, lonlatLog)  = spatial.getLonLatRDD(
            inputFile,
            args(1),
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

        logArrBuff ++= lonlatLog

        val hotSpots = new Hotspots()

        val (preHotSpotsArr, logHSBuff) = hotSpots.hotSpots(
            poiRDD,
            scoreCol,
            cell_size,
            partition_size_k,
            hs_top_k,
            hs_nb_cell_weight,
            hs_printAsUnionCells
        )

        //Re-Write Geometries to Source Coordinates.
        val hotSpotsArr = spatial.transformPolyCoords(preHotSpotsArr, target_crs, source_crs)

        logArrBuff ++= logHSBuff

        val outRDD = mySparkSession.sparkContext.parallelize(hotSpotsArr)
                                   .map(t => (t._1 + ";" + t._2 + ";" + t._3))
                                   .coalesce(1)

        outRDD.saveAsTextFile(hs_outputFile)


        val totalTime = "Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec"
        println(totalTime)

        logArrBuff += totalTime
        mySparkSession.sparkContext.parallelize(logArrBuff).coalesce(1).saveAsTextFile(outLogPath)


        mySparkSession.spark_session.stop()
    }
    
}
