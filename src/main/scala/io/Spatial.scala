package io

import DBPOI.DBPOI
import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import mySparkSession.mySparkSession
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.osgeo.proj4j.{BasicCoordinateTransform, CRSFactory, ProjCoordinate}


case class Spatial() extends Serializable {

    def transformPolyCoords(inArr: Array[(Int, Geometry, Double)],
                            source_crs: String = "EPSG:4326",
                            target_crs: String = "EPSG:4326"): Array[(Int, Geometry, Double)] = {

        if(source_crs != target_crs) {
            val geometryFactory = new GeometryFactory()

            val factory = new CRSFactory
            val srcCrs = factory.createFromName(source_crs)
            val dstCrs = factory.createFromName(target_crs)

            val crs_transform = new BasicCoordinateTransform(srcCrs, dstCrs)
            val srcCoord = new ProjCoordinate()
            val dstCoord = new ProjCoordinate()

            inArr.map {
                case (id, geom, score) => {
                    val new_coords = geom.getCoordinates.map {
                        coord => {
                            srcCoord.setValue(coord.x, coord.y)

                            //Tranform coordinates.
                            crs_transform.transform(srcCoord, dstCoord)
                            coord.x = dstCoord.x
                            coord.y = dstCoord.y

                            coord
                        }
                    }

                    val poly = geometryFactory.createPolygon(new_coords).asInstanceOf[Geometry]

                    (id, poly, score)
                }
            }
        }
        else{
            inArr
        }
    }


    def transformDBPOI_RDD_Coords(inRDD: RDD[(String, Array[DBPOI])],
                                  source_crs: String = "EPSG:4326",
                                  target_crs: String = "EPSG:4326"
                                 ) : RDD[(String, Array[DBPOI])] = {

        if(source_crs != target_crs) {

            inRDD.mapPartitions {
                p_iter => {

                    val factory = new CRSFactory
                    val srcCrs = factory.createFromName(source_crs)
                    val dstCrs = factory.createFromName(target_crs)

                    val crs_transform = new BasicCoordinateTransform(srcCrs, dstCrs)
                    val srcCoord = new ProjCoordinate()
                    val dstCoord = new ProjCoordinate()

                    val arrBuff = ArrayBuffer[(String, Array[DBPOI])]()

                    while (p_iter.hasNext) {
                        val (ci_name, cluster_i) = p_iter.next()

                        val newCoords = cluster_i.map {
                            dbpoi => {
                                srcCoord.setValue(dbpoi.lon, dbpoi.lat)

                                //Tranform coordinates.
                                crs_transform.transform(srcCoord, dstCoord)

                                val lon = dstCoord.x
                                val lat = dstCoord.y

                                val new_dbpoi = DBPOI(dbpoi.poiId, lon, lat)
                                new_dbpoi.clusterName = dbpoi.clusterName
                                new_dbpoi.dbstatus = dbpoi.dbstatus
                                new_dbpoi.isBoundary = dbpoi.isBoundary
                                new_dbpoi.isDense = dbpoi.isDense

                                new_dbpoi
                            }
                        }

                        arrBuff += ((ci_name, newCoords))
                    }

                    arrBuff.toIterator
                }
            }
        }
        else{
            inRDD
        }
    }


    //RDD[id, (lon, lat, HashMap[col_id, value])]
    def getLonLatRDD(inputFile: String,
                     epsg_file: String,
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
                    ) : (RDD[(String, (Double, Double, mutable.HashMap[String, Object]))], ArrayBuffer[String])= {

        //Error logging
        val logArrBuff = ArrayBuffer[String]()

        case class EPSG_Boundary(val minLon: Double,
                                 val minLat: Double,
                                 val maxLon: Double,
                                 val maxLat: Double
                                )

        //Array[(EPSG_code, EPSG_Boundary)]
        val epsgDB = mySparkSession.sparkContext
                                   .textFile(epsg_file)
                                   .collect()
                                   .map{
                                       str => {
                                           val arrT = str.split(";")
                                           val boundaries = arrT(1).split(",")
                                           val epsg_code = arrT(0).toInt

                                           (epsg_code, EPSG_Boundary(boundaries(0).toDouble, boundaries(1).toDouble, boundaries(2).toDouble, boundaries(3).toDouble))
                                       }
                                   }

        val source_epsg_code = source_crs.split(":")(1).toInt
        val target_epsg_code = target_crs.split(":")(1).toInt

        var source_epsg_boundary: Option[EPSG_Boundary] = None
        var target_epsg_boundary: Option[EPSG_Boundary] = None

        for {
            (epsg_code, epsg_boundary) <- epsgDB
        }{
            if(epsg_code == source_epsg_code)
                source_epsg_boundary = Some(epsg_boundary)
            if(epsg_code == target_epsg_code)
                target_epsg_boundary = Some(epsg_boundary)

        }

        source_epsg_boundary match {
            case Some(epsg) => logArrBuff += s"Source EPSG projected Boundaries: $epsg"
            case None       => logArrBuff += s"Source EPSG projected Boundaries WERE NOT FOUND in EPSG DataBase!"
        }

        target_epsg_boundary match {
            case Some(epsg) => logArrBuff += s"Target EPSG projected Boundaries: $epsg"
            case None       => logArrBuff += s"Target EPSG projected Boundaries WERE NOT FOUND in EPSG DataBase!"
        }


        val sourceOpt_epsg_boundary_bd = mySparkSession.sparkContext.broadcast(source_epsg_boundary)
        val targetOpt_epsg_boundary_bd = mySparkSession.sparkContext.broadcast(target_epsg_boundary)


        val pointRDD_0 = mySparkSession.sparkContext.textFile(inputFile, 100)
                                       .mapPartitionsWithIndex{
                                           (p_id, lineIter) => {

                                               val sourceEPSG_boundary = sourceOpt_epsg_boundary_bd.value.get
                                               val targetEPSG_boundary = targetOpt_epsg_boundary_bd.value.get

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

                                                       //Check to be inside Boundary Box.
                                                       if( lon < targetEPSG_boundary.minLon ||
                                                           lon > targetEPSG_boundary.maxLon ||
                                                           lat < targetEPSG_boundary.minLat ||
                                                           lat > targetEPSG_boundary.maxLat
                                                       )
                                                           throw new Exception(s"($id) ($lon, $lat) out of Bounds!")


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
        (pointRDD_0, logArrBuff)
    }
}

