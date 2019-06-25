package io

import java.io.{File, FileNotFoundException, IOException, PrintWriter}

import DBPOI.DBPOI
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTWriter
import org.apache.spark.rdd.RDD


object Out {

    //file Path         Can accept any Operation manipulating this File with a PrintWriter
    def writeToFile(file: java.io.File)(op: java.io.PrintWriter => Unit) = {

        //Create a Default PrintWriter in our case with UTF-8 charset
        val pw = new PrintWriter(file, "UTF-8")
        try{
            op(pw)
        }
        catch {
            case ex: FileNotFoundException => println(s"Write/Opening operation went wrong with file: ${file.getName}")
            case ex: Exception => {
                ex.printStackTrace()
                println(s"Something went wrong with file: ${file.getName}")
            }
        }
        finally {
            pw.close()
        }
    }

    def clusterToStr(cluster: Array[DBPOI]): String = {
        var s = ""
        for(poi <- cluster ){
            s = s + s", (${poi.lon} ${poi.lat})"
        }

        "MULTIPOINT (" + s.tail + ")"
    }

    def writeClusters(finalRDD: RDD[(String, Array[DBPOI])], outputFile: String) : Unit = {

        finalRDD.map{
            case (clusterName, cluster) => {
                clusterName + ";" + this.clusterToStr(cluster) + ";" + cluster.size
            }
        }
        //.coalesce(1)
        .saveAsTextFile(outputFile)
    }

    @throws[IOException]
    def write_hotspots(geomArr: Array[(Int, Geometry, Double)], outputFile: String, delimiter: String): Unit = {

        val wktWriter = new WKTWriter()

        //Output to File
        writeToFile(new File(outputFile)) {
            pw: PrintWriter => {
                geomArr.foreach{
                    case (id, geom, score) => pw.println(id + delimiter + wktWriter.write(geom) + delimiter + score )
                }
            }
        }
    }

}

