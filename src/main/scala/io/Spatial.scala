package io

import mySparkSession.mySparkSession
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.osgeo.proj4j.{BasicCoordinateTransform, CRSFactory, ProjCoordinate}


case class Spatial() extends Serializable {


    //RDD[id, (lon, lat, HashMap[col_id, value])]
    def getLonLatRDD(inputFile: String,
                     id_Col      : String,
                     lon_Col     : String,
                     lat_Col     : String,
                     score_Col   : String,
                     kwd_Col     : String,
                     user_kwds   : Array[String],
                     colMap      : scala.collection.immutable.Map[String, Int],
                     other_Cols  : Array[String],
                     col_Sep     : String,
                     kwd_sep     : String =",",

                     source_crs  : String = "EPSG:4326",
                     target_crs  : String = "EPSG:4326"
                    ) : RDD[(String, (Double, Double, mutable.HashMap[String, Object]))] = {


        val pointRDD_0 = mySparkSession.sparkContext.textFile(inputFile)
                                       .mapPartitionsWithIndex{
                                           (p_id, lineIter) => {

                                               val factory = new CRSFactory
                                               val srcCrs = factory.createFromName(source_crs)
                                               val dstCrs = factory.createFromName(target_crs)

                                               val crs_transform = new BasicCoordinateTransform(srcCrs, dstCrs)

                                               val srcCoord = new ProjCoordinate()
                                               val dstCoord = new ProjCoordinate()

                                               val arr_buff = ArrayBuffer[(String, (Double, Double, mutable.HashMap[String, Object]))]()
                                               var i = 0

                                               while (lineIter.hasNext){
                                                   val line = lineIter.next()

                                                   try{
                                                       val arr = line.split(col_Sep)
                                                       val userData = mutable.HashMap[String, Object]()

                                                       var id = ""
                                                       if(id_Col != "")
                                                           id = arr(colMap(id_Col))
                                                       else {
                                                           id = p_id + "p" + i
                                                           i = i + 1
                                                       }

                                                       var lon = arr(colMap(lon_Col)).toDouble
                                                       var lat = arr(colMap(lat_Col)).toDouble

                                                       //Transform Coordinates if source-target EPSG is defined.
                                                       if(source_crs != target_crs){

                                                           srcCoord.setValue(lon, lat)

                                                           //Tranform coordinates.
                                                           crs_transform.transform(srcCoord, dstCoord)

                                                           lon = dstCoord.x
                                                           lat = dstCoord.y
                                                       }

                                                       var score = 1.0

                                                       if(score_Col != "__score_col__")
                                                           score = arr(colMap(score_Col)).toDouble

                                                       userData += ((score_Col, Double.box(score)))

                                                       var kwds = Array[String]()
                                                       if(kwd_Col != "") {
                                                           kwds = arr(colMap(kwd_Col)).split(kwd_sep)
                                                           if(user_kwds.isEmpty) {
                                                               userData += ((kwd_Col, kwds))
                                                               userData += (("__include_poi__", Boolean.box(true)))
                                                           }
                                                           else{ //User has specified keywords.
                                                               if(kwds.intersect(user_kwds).nonEmpty) {
                                                                   userData += ((kwd_Col, kwds))
                                                                   userData += (("__include_poi__", Boolean.box(true)))
                                                               }
                                                               else
                                                                   userData += (("__include_poi__", Boolean.box(false)))
                                                           }
                                                       }
                                                       else
                                                           userData += (("__include_poi__", Boolean.box(true)))

                                                       if(other_Cols.nonEmpty){
                                                           other_Cols.foreach{
                                                               name => userData += ((name, arr(colMap(name))))
                                                           }
                                                       }
                                                       //Include all Columns.
                                                       else{
                                                           colMap.foreach{
                                                               case (key, value) => {
                                                                   if(key != id_Col && key != lon_Col && key != lat_Col && key != score_Col && key != kwd_Col)
                                                                       userData += ((key, arr(value)))
                                                               }
                                                           }
                                                       }

                                                       arr_buff += ((id, (lon, lat, userData)))
                                                   }
                                                   catch {
                                                       case e: Exception => {
                                                           //Do Nothing!
                                                       }
                                                   }
                                               }

                                               arr_buff.toIterator
                                           }
                                       }
        pointRDD_0
    }

}

