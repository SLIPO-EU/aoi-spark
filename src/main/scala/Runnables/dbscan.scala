package Runnables

import DBSCAN.DBSCAN
import io.{InputFileParser, Spatial}
import mySparkSession.mySparkSession
import io.Out
import org.apache.spark.storage.StorageLevel._
import scala.collection.mutable.ArrayBuffer


object dbscan {

    def main(args: Array[String]): Unit = {

        val logArrBuff: ArrayBuffer[String] = ArrayBuffer[String]()
        val startTime = System.nanoTime()

        if(args.size != 1){
            println("You should give the properties FilePath as argument to main...")
            return
        }

        val inputFileParser = new InputFileParser(args(0))
        val (finalCheck, inputLog) = inputFileParser.loadPropertiesFile()

        logArrBuff ++= inputLog

        if(!finalCheck){
            return
        }

        val inputFile  = inputFileParser.getInputFile()
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

        //DBSCAN Parameters
        val dbeps  = inputFileParser.getEpsilon()
        val minPts = inputFileParser.getMinPts()

        //EPSG
        val source_crs = inputFileParser.getSourceCrs()
        val target_crs = inputFileParser.getTargetCrs()

        //Log Path
        val outLogPath = cl_outputFile + "/Log"

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
        val finalRDD = dbscan.dbscan(pointRDD = poiRDD, eps = dbeps, minPts = minPts).persist(MEMORY_AND_DISK)

        //Write Clusters to Output FIle.
        Out.writeClusters(finalRDD, cl_outputFile)

        val (numOfClusters, numOfClusteredPois) = finalRDD.aggregate(0, 0)(
            (z, x) => (z._1 + 1, z._2 + x._2.size),
            (z1, z2) => (z1._1 + z2._1, z1._2 + z2._2)
        )

        val totalTime = "Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec"

        logArrBuff += s"NumOfClusters = $numOfClusters"
        logArrBuff += s"NumOfClusteredPois = $numOfClusteredPois"
        logArrBuff += totalTime

        mySparkSession.sparkContext.parallelize(logArrBuff).coalesce(1).saveAsTextFile(outLogPath)

        dbscan.clear()
        finalRDD.unpersist(true)

        mySparkSession.spark_session.stop()
    }

}

