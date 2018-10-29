package runnables

import HotSpots._
import MySparkContext.mySparkContext
import gr.athenarc.imsi.slipo.analytics.loci.io.ResultsWriter
import io.InputFileParser

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

        val colSep       = inputFileParser.getCol_Sep()
        val keyWordSep   = inputFileParser.getkeyWord_Sep()
        val userKeywords = inputFileParser.getUserKeyWords()

        val cell_size        = inputFileParser.getCellSize()
        val partition_size_k = inputFileParser.getPartitionSizeK()

        val hs_top_k             = inputFileParser.getHS_Top_k()
        val hs_nb_cell_weight    = inputFileParser.getHS_CellWeight()
        val hs_printAsUnionCells = inputFileParser.getHS_printAsUnionCells()

        val hotSpots = new Hotspots()

        //List[SpatialObject]
        val hotSpotsLst = hotSpots.hotSpots(
                                        inputFile,

                                        lonCol,
                                        latCol,
                                        scoreCol,
                                        keyWordCol,
                                        userKeywords,

                                        colSep,
                                        keyWordSep,

                                        cell_size,
                                        partition_size_k,

                                        hs_top_k,
                                        hs_nb_cell_weight,
                                        hs_printAsUnionCells
                                     )


        val resultsWriter: ResultsWriter = new ResultsWriter()
        resultsWriter.write(hotSpotsLst, hs_outputFile, colSep)


        println("Total Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        mySparkContext.sc.stop()
    }

}



