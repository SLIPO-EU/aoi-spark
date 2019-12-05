package io

import mySparkSession.mySparkSession
import scala.collection.mutable.ArrayBuffer

/*
* Here you Manage, what to Read from Properties File.
*
* */
@SerialVersionUID(12L)
class InputFileParser(val propertiesFile: String) extends Serializable {

    //I/O Files
    protected var inputFile  = ""
    protected var hs_outputFile = ""
    protected var cl_outputFile = ""

    //Columns
    protected var id_Col      = ""
    protected var lon_Col     = ""
    protected var lat_Col     = ""
    protected var score_Col   = ""
    protected var keyword_Col = ""
    protected var other_Cols  = Array[String]()

    protected var colMap      = Map[String, Int]()

    /*
    * Delimeters
    */
    //Column Seperator
    protected var col_Sep: String = ";"
    //KeyWord Seperator
    protected var keyword_Sep : String = ","

    //User Specified KeyWords for pre-Filtering
    protected var user_Keywords = Array.empty[String]

    /*
    * Custom Grid partition Handling variables.
    *
    * cell_gs : cell-width
    * pSize_k : partition-size, should be k(Integer) number of times the cell-width.
    */
    private var cell_gs = 0.005
    private var pSize_k = 50

    /*
    * HotSpot Variables
    */
    private var hs_top_k = 50
    private var hs_nb_cell_weight = 1.0
    private var hs_print_as_unioncells = false

    //DSCAN Variables
    private var db_eps = 0.01
    private var minPts = 10

    //LDA Variables
    private var numOfTopics = 5

    private var source_crs = "EPSG:4326"
    private var target_crs = "EPSG:4326"


    def isInt(str: String) = {
        try{
            str.toInt
            true
        }
        catch {
            case e: Exception => {
                false
            }
        }
    }

    @throws(classOf[Exception])
    def loadPropertiesFile() : (Boolean, ArrayBuffer[String]) = {

        //Error logging
        val logArrBuff = ArrayBuffer[String]()

        val propertiesRDD = mySparkSession.sparkContext.textFile(this.propertiesFile)
        val propArr = propertiesRDD.collect().filter(s => s.contains("=") && !s.startsWith("#"))

        try {
            for(line <- propArr){
                val arr = line.split("=", 2).map(s => s.trim)
                arr(0) match {
                    case "input_file" => this.inputFile = arr(1)
                    case "hs-output_file" => this.hs_outputFile = arr(1)
                    case "cl-output_file" => this.cl_outputFile = arr(1)
                    case "column_id" => this.id_Col = arr(1)
                    case "column_lon" => this.lon_Col = arr(1)
                    case "column_lat" => this.lat_Col = arr(1)

                    //Optional
                    case "column_score" => {
                        arr(1) match {
                            case "" => this.score_Col = "__score_col__"
                            case s  => this.score_Col = s
                        }
                    }
                    case "column_keywords" => this.keyword_Col = arr(1)
                    case "other_cols" => {
                        arr(1) match {
                            case "" => this.other_Cols = Array.empty[String]
                            case s  => this.other_Cols = s.split(",")
                        }
                    }
                    //Delimeters
                    case "csv_delimiter" => this.col_Sep = arr(1)
                    case "keyword_delimiter" => this.keyword_Sep = arr(1)

                    //User specified KeyWords
                    case "keywords" => {
                        arr(1) match {
                            case "" => this.user_Keywords = Array.empty[String]
                            case s  => this.user_Keywords = s.split(",")
                        }
                    }

                    // Custom Grid Variables
                    case "cell-eps" => this.cell_gs = arr(1).toDouble
                    case "pSize_k"  => this.pSize_k = arr(1).toInt

                    //HotSpot Variables
                    case "hs-top-k"               => {
                        arr(1) match {
                            case "" => this.hs_top_k = -1
                            case s  => this.hs_top_k = arr(1).toInt
                        }
                    }
                    case "hs-nb-cell-weight"      => this.hs_nb_cell_weight = arr(1).toDouble
                    case "hs-print-as-unioncells" => this.hs_print_as_unioncells = arr(1).toBoolean

                    // DBSCAN Variables
                    case "cl-eps"    => this.db_eps = arr(1).toDouble
                    case "cl-minPts" => this.minPts = arr(1).toInt

                    // LDA Parameters
                    case "numOfTopics" => this.numOfTopics = arr(1).toInt

                    //Source and Target crs.
                    case "source_crs"  => {
                        arr(1) match {
                            case "" => ()
                            case s  => this.source_crs = s
                        }
                    }
                    case "target_crs" => {
                        arr(1) match {
                            case "" => ()
                            case s  => this.target_crs = s
                        }
                    }
                }
            }

            val firstLine = mySparkSession.sparkContext.textFile(this.inputFile).first()
            val colArr = firstLine.split(this.col_Sep).zipWithIndex

            //If all Columns are expressed as Integers
            if(isInt(this.lon_Col)) {
                this.colMap = colArr.map(t => (t._2.toString, t._2)).toMap
            } else {   //We have Column Names as Strings.
                this.colMap = colArr.toMap
            }

            //General Variables
            logArrBuff += "Input File = " + this.inputFile
            logArrBuff += "HS-Output File = " + this.hs_outputFile
            logArrBuff += "CL-Output File = " + this.cl_outputFile
            logArrBuff += "ID Col = " + this.id_Col
            logArrBuff += "Lon Col = " + this.lon_Col
            logArrBuff += "lat Col = " + this.lat_Col
            logArrBuff += "Score Col = " + this.score_Col
            logArrBuff += "keyWord Col = " + this.keyword_Col
            logArrBuff += "Other Columns = " + this.other_Cols.mkString(",")

            logArrBuff += "Col Sep = " + this.col_Sep
            logArrBuff += "KeyWord Sep = " + this.keyword_Sep
            logArrBuff += "User keyWords = " + this.user_Keywords.mkString(",")

            //Custom Grid Variables
            logArrBuff += s"Cell size = ${this.cell_gs}"
            logArrBuff += s"Partition size  = ${this.pSize_k * this.cell_gs}"

            //HotSpot Variables
            logArrBuff += s"HotSpots Top-k                  = ${this.hs_top_k}"
            logArrBuff += s"HotSpots NB_cell_weight         = ${this.hs_nb_cell_weight}"
            logArrBuff += s"HotSpots hs-print-as-unioncells = ${this.hs_print_as_unioncells}"


            //DBSCAN Variables
            logArrBuff += s"DBSCAN epsilon = ${this.db_eps}"
            logArrBuff += s"DBSCAN minPts  = ${this.minPts}"

            //LDA Variales
            logArrBuff += s"LDA NumOfTopics = ${this.numOfTopics}"


            logArrBuff += s"Source crs = ${this.source_crs}"
            logArrBuff += s"Target crs = ${this.target_crs}"
            logArrBuff += "\n"

            (true, logArrBuff)
        }
        catch {
            case e: Exception => {
                logArrBuff += s"Error: ${e.printStackTrace()}"
                e.printStackTrace()
                println("An Error occured while oppening and reading the General Properties File! Please try again!")
                (false, logArrBuff)
            }
        }
        finally {
        }
    }


    def getPropertiesFile() : String = this.propertiesFile
    def getID_Col(): String = this.id_Col
    def getLon_Col(): String = this.lon_Col
    def getLat_Col(): String = this.lat_Col
    def getScore_Col(): String = this.score_Col
    def getOtherCols(): Array[String] = this.other_Cols

    def getkeyWord_Col(): String = this.keyword_Col
    def getColMap(): scala.collection.immutable.Map[String, Int] = this.colMap

    def getCol_Sep(): String = this.col_Sep
    def getkeyWord_Sep(): String = this.keyword_Sep
    def getUserKeyWords(): Array[String] = this.user_Keywords

    def getInputFile(): String = this.inputFile
    def getHS_OutputFile(): String = this.hs_outputFile
    def getCL_OutputFile(): String = this.cl_outputFile

    //Custom Grid Getters
    def getCellSize()       : Double  = this.cell_gs
    def getPartitionSizeK() : Int = this.pSize_k

    //HotSpots
    def getHS_Top_k() : Int = this.hs_top_k
    def getHS_CellWeight(): Double = this.hs_nb_cell_weight
    def getHS_printAsUnionCells(): Boolean = this.hs_print_as_unioncells

    //DBSCAN
    def getEpsilon() : Double = this.db_eps
    def getMinPts()  : Int    = this.minPts

    //LDA
    def getNumOfTopics(): Int = this.numOfTopics

    //EPSG
    def getSourceCrs(): String = this.source_crs
    def getTargetCrs(): String = this.target_crs

}

