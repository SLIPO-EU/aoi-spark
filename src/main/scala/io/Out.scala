package io

import java.io.{FileNotFoundException, PrintWriter}

import DBPOI.DBPOI
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
        .saveAsTextFile(outputFile)
    }
}


