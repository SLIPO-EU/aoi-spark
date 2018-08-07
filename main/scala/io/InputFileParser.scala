package io

import java.io.{FileInputStream, FileNotFoundException, PrintWriter}
import java.util.Properties


/*
* Here you Manage, what to Read from Properties File.
*
* */
@SerialVersionUID(12L)
class InputFileParser(val propertiesFile: String) extends Serializable {

    //I/O Files
    protected var inputFile  = ""
    protected var outputFile = ""

    //Columns
    protected var id_Col      = -1
    protected var lon_Col     = -1
    protected var lat_Col     = -1



    /*
    * Delimeters
    */

    //Column Seperator
    protected var col_Sep : String = ""


    /*
    * Boundary Box
    */
    protected var minLon = -180.0
    protected var maxLon =  180.0

    protected var minLat = -90.0
    protected var maxLat =  90.0


    /*
    * DBSCAN Variables
    *
    */
    private var epsilon = 0.5
    private var minPts  = 10


    private var lambda  = 10



    @throws(classOf[Exception])
    def loadPropertiesFile() : Boolean = {

        try {

            val prop  = new Properties()
            val inputStream = new FileInputStream(propertiesFile)

            //Load the Properties File
            prop.load(inputStream)

            //I/O Files
            this.inputFile  = prop.getProperty("InputFile")
            this.outputFile = prop.getProperty("OutputFile")


            //The Columns
            this.id_Col  = prop.getProperty("IdCol") match {
                case "" => -1
                case s  => s.toInt - 1
            }

            this.lon_Col  = prop.getProperty("LonCol").toInt - 1     //Required!
            this.lat_Col  = prop.getProperty("LatCol").toInt - 1     //Required!


            //Delimeters
            this.col_Sep  = prop.getProperty("Column_sep")



            //Boundary Box
            prop.getProperty("minLongitude") match {
                case "" => ()
                case s  => this.minLon = s.toDouble
            }

            prop.getProperty("maxLongitude") match {
                case "" => ()
                case s  => this.maxLon = s.toDouble
            }

            prop.getProperty("minLatitude") match {
                case "" => ()
                case s  => this.minLat = s.toDouble
            }

            prop.getProperty("maxLatitude") match {
                case "" => ()
                case s  => this.maxLat = s.toDouble
            }


            /*
            * DBSCAN Variables
            *
            */
            this.epsilon = prop.getProperty("epsilon").toDouble
            this.minPts  = prop.getProperty("minPts").toInt

            this.lambda    = prop.getProperty("lambda").toInt




            //General Variables
            println("Input File = " + this.inputFile)
            println("Output File = " + this.outputFile)
            println("ID Col = " + this.id_Col)
            println("Lon Col = " + this.lon_Col)
            println("lat Col = " + this.lat_Col)

            println("Col Sep = " + this.col_Sep)


            println("MinLon = " + this.minLon)
            println("MaxLon = " + this.maxLon)
            println("MinLat = " + this.minLat)
            println("MaxLat = " + this.maxLat)


            //DBSCAN Variables
            println("DBSCAN epsilon = " + this.epsilon)
            println("DBSCAN MinPts  = " + this.minPts)


            println("\nLambda = " + this.lambda)



            inputStream.close()

            true

        }
        catch {
            case e: Exception => {
                println("An Error occured while oppening and reading the General Properties From Properties File! Please try again!")
                false
            }
        }
        finally {

        }
    }


    def getIFP(): InputFileParser ={
        this
    }


    def getPropertiesFile() : String = {
        this.propertiesFile
    }


    def getID_Col(): Int = this.id_Col
    def getLon_Col(): Int = this.lon_Col
    def getLat_Col(): Int = this.lat_Col


    def getColSep(): String = this.col_Sep


    def getMinLon(): Double = this.minLon
    def getMaxLon(): Double = this.maxLon

    def getMinLat(): Double = this.minLat
    def getMaxLat(): Double = this.maxLat


    def getInputFile(): String = this.inputFile
    def getOutputFile(): String = this.outputFile



    def getGridPartitionSize() : Double = this.lambda * this.epsilon


    def getLambda(): Int = this.lambda


    //DBSCAN
    def getDBSCAN_epsilon() : Double = this.epsilon
    def getDBSCAN_minPts()  : Int    = this.minPts



}



























