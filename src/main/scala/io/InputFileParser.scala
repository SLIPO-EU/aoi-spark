package io

import java.io.FileInputStream
import java.util.Properties

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
    protected var id_Col      = -1
    protected var lon_Col     = -1
    protected var lat_Col     = -1
    protected var score_Col   = -1
    protected var keyword_Col = -1

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
            this.id_Col  = prop.getProperty("column_id") match {
                case "" => -1
                case s  => s.toInt - 1
            }

            this.lon_Col  = prop.getProperty("column_lon").toInt - 1     //Required!
            this.lat_Col  = prop.getProperty("column_lat").toInt - 1     //Required!

            //Optional
            this.score_Col   = prop.getProperty("column_score") match {
                case "" => -1
                case s  => s.toInt - 1
            }

            //Optional
            this.keyword_Col = prop.getProperty("column_keywords") match {
                case "" => -1
                case s  => s.toInt - 1
            }

            //Delimeters
            this.col_Sep     = prop.getProperty("csv_delimiter")
            this.keyword_Sep = prop.getProperty("keyword_delimiter")


            //User specified KeyWords
            prop.getProperty("keywords") match {
                case "" => this.user_Keywords = Array.empty[String]
                case s  => this.user_Keywords = s.split(",")
            }

            if(this.user_Keywords.nonEmpty && this.keyword_Col == -1){
                println("You have specified keywords for Filtering, But you haven't specified what is the KeyWord Column, in the Column Order.")
                throw new Exception
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


            //General Variables
            println("Input File = " + this.inputFile)
            println("HS-Output File = " + this.hs_outputFile)
            println("CL-Output File = " + this.cl_outputFile)
            println("ID Col = " + this.id_Col)
            println("Lon Col = " + this.lon_Col)
            println("lat Col = " + this.lat_Col)
            println("Score Col = " + this.score_Col)
            println("keyWord Col = " + this.keyword_Col)

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
    def getID_Col(): Int = this.id_Col
    def getLon_Col(): Int = this.lon_Col
    def getLat_Col(): Int = this.lat_Col
    def getScore_Col(): Int = this.score_Col
    def getkeyWord_Col(): Int = this.keyword_Col

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

}




