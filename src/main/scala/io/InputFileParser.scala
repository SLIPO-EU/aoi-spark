package io

import java.io.FileInputStream
import java.util.Properties
import scala.io.Source

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
    def loadPropertiesFile() : Boolean = {

        try {

            val prop  = new Properties()
            val inputStream = new FileInputStream(propertiesFile)

            //Load the Properties File
            prop.load(inputStream)

            //I/O Files
            this.inputFile  = prop.getProperty("input_file")
            this.hs_outputFile = prop.getProperty("hs-output_file")
            this.cl_outputFile = prop.getProperty("cl-output_file")

            //The Columns
            this.id_Col  = prop.getProperty("column_id")

            this.lon_Col  = prop.getProperty("column_lon")
            this.lat_Col  = prop.getProperty("column_lat")

            //Optional
            prop.getProperty("column_score") match {
                case "" => this.score_Col = "__score_col__"
                case s  => this.score_Col = s
            }

            //Optional
            this.keyword_Col = prop.getProperty("column_keywords")

            //Other Columns to Keep
            prop.getProperty("other_cols") match {
                case "" => this.other_Cols = Array.empty[String]
                case s  => this.other_Cols = s.split(",")
            }

            //Delimeters
            this.col_Sep     = prop.getProperty("csv_delimiter")
            this.keyword_Sep = prop.getProperty("keyword_delimiter")

            //User specified KeyWords
            prop.getProperty("keywords") match {
                case "" => this.user_Keywords = Array.empty[String]
                case s  => this.user_Keywords = s.split(",")
            }

            /*
            * Custom Grid Variables
            */
            this.cell_gs = prop.getProperty("cell-eps").toDouble
            this.pSize_k = prop.getProperty("pSize_k").toInt

            /*
            * HotSpot Variables
            */
            this.hs_top_k = prop.getProperty("hs-top-k").toInt
            this.hs_nb_cell_weight = prop.getProperty("hs-nb-cell-weight").toDouble
            this.hs_print_as_unioncells = prop.getProperty("hs-print-as-unioncells").toBoolean

            /*
            * DBSCAN Variables
            */
            this.db_eps = prop.getProperty("cl-eps").toDouble
            this.minPts = prop.getProperty("cl-minPts").toInt


            /*
            * LDA Parameters
            */
            this.numOfTopics = prop.getProperty("numOfTopics").toInt


            val firstLine = {
                val src = Source.fromFile(inputFile)
                val line = src.getLines.next()
                src.close
                line
            }

            val colArr = firstLine.split(col_Sep).zipWithIndex

            //If all Columns are expressed as Integers
            if(isInt(lon_Col))
                this.colMap = colArr.map(t => (t._2.toString, t._2)).toMap

            else   //We have Column Names as Strings.
                this.colMap = colArr.toMap


            prop.getProperty("source_crs") match {
                case "" => ()
                case s  => this.source_crs = s
            }

            prop.getProperty("target_crs") match {
                case "" => ()
                case s  => this.target_crs = s
            }


            //println(this.colMap)

            //General Variables
            println("Input File = " + this.inputFile)
            println("HS-Output File = " + this.hs_outputFile)
            println("CL-Output File = " + this.cl_outputFile)
            println("ID Col = " + this.id_Col)
            println("Lon Col = " + this.lon_Col)
            println("lat Col = " + this.lat_Col)
            println("Score Col = " + this.score_Col)
            println("keyWord Col = " + this.keyword_Col)
            println("Other Columns = " + this.other_Cols.mkString(","))

            println("Col Sep = " + this.col_Sep)
            println("KeyWord Sep = " + this.keyword_Sep)
            println("User keyWords = " + this.user_Keywords.mkString(","))

            //Custom Grid Variables
            println(s"Cell size = ${this.cell_gs}")
            println(s"Partition size  = ${this.pSize_k * this.cell_gs}")

            //HotSpot Variables
            println(s"HotSpots Top-k                  = ${this.hs_top_k}")
            println(s"HotSpots NB_cell_weight         = ${this.hs_nb_cell_weight}")
            println(s"HotSpots hs-print-as-unioncells = ${this.hs_print_as_unioncells}")


            //DBSCAN Variables
            println(s"DBSCAN epsilon = ${this.db_eps}")
            println(s"DBSCAN minPts  = ${this.minPts}")

            //LDA Variales
            println(s"LDA NumOfTopics = ${this.numOfTopics}")


            println(s"Source crs = ${this.source_crs}")
            println(s"Target crs = ${this.target_crs}")


            inputStream.close()
            true
        }
        catch {
            case e: Exception => {
                e.printStackTrace()
                println("An Error occured while oppening and reading the General Properties File! Please try again!")
                false
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


