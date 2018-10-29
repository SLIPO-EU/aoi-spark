package DBSCAN

/*
* DBSCAN Distributed Edition in Spark & Scala.
*
* Authors: Panagiotis Kalampokis, Dr. Dimitris Skoutas
* */

import DBPOI.DBPOI
import MySparkContext.mySparkContext
import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import gr.athenarc.imsi.slipo.analytics.loci.POI

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.mutable.{ArrayBuffer, HashMap}


class DBSCAN() extends Serializable {

    private var clusterRDD: RDD[DBPOI] = null
    private var mergingClusterNameVecBD: Broadcast[Vector[Set[String]]] = null
    private var boundaryPoisToKeepHMBD : Broadcast[HashMap[String, String]] = null
    private var spatialPartitionerBD   : Broadcast[SpatialPartitioner] = null

    private def areIntersectingSets(set1: Set[String], set2: Set[String]): Boolean = {
        set1.exists(s1 => set2.exists(s2 => s1 == s2) )
    }

    private def insertSetIntoVec(vec: Vector[Set[String]], xSet: Set[String]): Vector[Set[String]] = {

        var tmpVec = Vector[Set[String]]()
        var unionSet = Set[String]() ++ xSet

        for(set_i <- vec){
            if(areIntersectingSets(set_i, unionSet))
                unionSet = unionSet ++ set_i
            else
                tmpVec = tmpVec :+ set_i
        }
        unionSet +: tmpVec
    }

    protected def getExpandedEnvelopeFromPoint(p: Point, epsilon: Double): Envelope = {
        val env = p.getEnvelopeInternal
        env.expandBy(epsilon)

        env
    }

    /*
    * Performs DBSCAN in an RDD[Pois]
    *
    * */
    def dbscan(//An RDD of Pois
               poiRDD: RDD[POI],

               //DBSCAN Parameters.
               eps: Double,
               minPts: Int
              ) = {

        val pointRDD = poiRDD.map{
            poi => {
                val point = poi.getPoint

                point.setUserData(poi.getId)
                point
            }
        }

        dbclusters(pointRDD, eps, minPts)
    }

    /*
    * Performs DBSCAN from the input File & Columns.
    *
    */
    def dbscan(inputFile: String,

               id_Col      : Int,
               lon_Col     : Int,
               lat_Col     : Int,
               keyword_Col : Int,

               //Seperators
               col_Sep     : String,
               keywordSep  : String,

               //User Keywords.
               userKeyWords: Array[String],

               //DBSCAN Parameters.
               eps: Double,
               minPts: Int

              ) = {

        //RDD[Point]
        val pointRDD = mySparkContext.sc.textFile(inputFile)
                                     .mapPartitions{
                                         lineIter => {
                                             val geometryFactory = new GeometryFactory()

                                             lineIter.map{
                                                 line => {
                                                     try{
                                                         val arr = line.split(col_Sep)

                                                         //If User has specified keywords.
                                                         if (keyword_Col >= 0 && userKeyWords.nonEmpty ) {
                                                             val poi_keywords = arr(keyword_Col).split(keywordSep)

                                                             //If poi meets at least one of specified keywords, include it!
                                                             if(poi_keywords.intersect(userKeyWords).nonEmpty){

                                                                 val id  = arr(id_Col)
                                                                 val lon = arr(lon_Col).toDouble
                                                                 val lat = arr(lat_Col).toDouble
                                                                 val point = geometryFactory.createPoint(new Coordinate(lon, lat))

                                                                 point.setUserData(id)
                                                                 point
                                                             }
                                                             else{  //Else If Poi doesn't meet the specified keywords, Sent it to Erroneous Queue...
                                                                 val point = geometryFactory.createPoint(new Coordinate())

                                                                 point.setUserData("error")
                                                                 point
                                                             }
                                                         }
                                                         else{  //Include them all
                                                             val id = arr(id_Col)
                                                             val lon = arr(lon_Col).toDouble
                                                             val lat = arr(lat_Col).toDouble
                                                             val point = geometryFactory.createPoint(new Coordinate(lon, lat))

                                                             point.setUserData(id)
                                                             point
                                                         }
                                                     }
                                                     catch {
                                                         case e: Exception => {
                                                             val point = geometryFactory.createPoint(new Coordinate())
                                                             point.setUserData("error")

                                                             point
                                                         }
                                                     }
                                                 }
                                             }
                                         }
                                     }
                                     //Filter Out all Erroneous Pois.
                                     .filter(point => point.getUserData.asInstanceOf[String] != "error")

        dbclusters(pointRDD, eps, minPts)
    }


    /*
    * Performs DBSCAN and Returns the clusters.
    * */
    def dbclusters(pointRDD_0: RDD[Point], eps: Double, minPts: Int) = {

        val pointRDD_1 = new JavaRDD[Point](pointRDD_0)
        val pointRDD = new PointRDD(pointRDD_1)

        pointRDD.analyze()

        //Perform Spatial Partitioning with QuadTree
        pointRDD.spatialPartitioning(GridType.QUADTREE, 16)

        //val boundaryEnvelopes = pointRDD.getPartitioner.getGrids
        //writeBoundaryEnvsToFile(pointRDD, outputFile + "_Envelopes_only.txt", geometryFactory)
        this.spatialPartitionerBD = mySparkContext.sc.broadcast(pointRDD.getPartitioner)

        //RDD[partitionID, dbpoi]
        val flatMappedRDD = pointRDD.spatialPartitionedRDD
                                    .rdd
                                    .mapPartitions{
                                        pointIter => {
                                            val geometryFactory = new GeometryFactory()
                                            pointIter.flatMap{
                                                point => {
                                                    //Get expanded by eps Envelope From Point.
                                                    val pointEnv = getExpandedEnvelopeFromPoint(point, eps)

                                                    //Given a Geometry, it Returns a List of Partitions it overlaps.
                                                    val pIDListTuple = this.spatialPartitionerBD.value.placeObject(geometryFactory.toGeometry(pointEnv))

                                                    //ArrayBuffer[PIDs]
                                                    val arrBuff = ArrayBuffer[Int]()

                                                    while (pIDListTuple.hasNext) {
                                                        val (pID, envP) = pIDListTuple.next()
                                                        arrBuff.append(pID.intValue())
                                                    }

                                                    //Is Boundary Point?
                                                    val isBoundaryP = (arrBuff.size > 1)

                                                    arrBuff.map{
                                                        pID => {
                                                            val poi = DBPOI(point.getUserData.asInstanceOf[String], point.getX, point.getY)
                                                            if (isBoundaryP)
                                                                poi.isBoundary = true

                                                            (pID, poi)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

        //RDD[(pID, ArrayBuffer[DBPOI])]
        val partitionRDD = flatMappedRDD.aggregateByKey(ArrayBuffer[DBPOI]())(
            //SeqOp
            (zArrBuffDBPoi, poi) => zArrBuffDBPoi += poi,

            //CombOp
            (zArrBuffDBPoi1, zArrBuffDBPoi2) => zArrBuffDBPoi1 ++= zArrBuffDBPoi2
        )


        //RDD[dbpoi]
        this.clusterRDD = partitionRDD.flatMap{
            case (pID, poiArrBuff) => {

                //New DBSCAN CLusterer For Each Partition-Envelope
                val dbclusterer = DBCLusterer(eps, minPts)

                //Perform DBSCAN in each partition and return a List of Clusters: ArrayBuffer[ArrayBuffer[DBPOI]]
                val clusters = dbclusterer.clusterPois(poiArrBuff)

                var i = 0
                for(cluster <- clusters){
                    for(poi <- cluster){
                        poi.clusterName = pID + "p" + i
                    }

                    i = i + 1
                }

                clusters.flatten
            }
        }
        .persist(MEMORY_AND_DISK)


        // Take all Boundary Pois.
        // RDD[poiID, dbpoi]
        val boundaryPoiRDD = this.clusterRDD.filter(_.isBoundary).map(poi => (poi.poiId, poi) )


        //RDD[poiID, (List[pID&cID], isDense?)]      Set[pID&cID], isDensePoi?
        val bPoiRDD = boundaryPoiRDD.aggregateByKey( (Set[String](), false) )(
            //SeqOp
            (zTuple, poi) => (zTuple._1 + poi.clusterName, zTuple._2 | poi.isDense ),

            //CombOp
            (tuple1, tuple2) => (tuple1._1 ++ tuple2._1, tuple1._2 | tuple2._2)
        )


        //   Vector[Set[pID&cIID]],  HashMap[poiID ,pID&cID]                    Vector[Set[pID&cIID]] , HashMap[poiID , pID&cID]
        val (mergingClusterNameVec, boundaryPoisToKeepHM) = bPoiRDD.aggregate( ( Vector[Set[String]](), HashMap[String, String]() ))(
            //SeqOp
            (zTuple, xTuple) => {

                val (vec, zHashMap) = zTuple

                //  [poiID, (Set[pID&cID], isDense?)]
                val (poiID, (pIDcIDSet, isDense)) = xTuple

                if(isDense)
                    (insertSetIntoVec(vec, pIDcIDSet), zHashMap)
                else
                    (vec, zHashMap += ((poiID, pIDcIDSet.head)) )
            },

            //CombOp
            (zTuple1, zTuple2) => {
                val (vec1, hashMap1) = zTuple1
                val (vec2, hashMap2) = zTuple2
                val vec3 = vec2.foldLeft(vec1)((zVec, xSet) => insertSetIntoVec(zVec, xSet))

                (vec3, hashMap1 ++= hashMap2)
            }
        )


        //Broadcast commonNames and PoisToKeep
        this.mergingClusterNameVecBD = mySparkContext.sc.broadcast(mergingClusterNameVec)
        this.boundaryPoisToKeepHMBD  = mySparkContext.sc.broadcast(boundaryPoisToKeepHM)

        val preFinalClusterRDD = this.clusterRDD.mapPartitions{
            poiIter => {

                val commonNameMap = this.mergingClusterNameVecBD.value.flatMap{
                    nameSet => {
                        val commonName = nameSet.toSeq.sortWith(_ < _).mkString("c")
                        nameSet.map(_ -> commonName)
                    }
                }.toMap

                poiIter.flatMap{
                    poi => {
                        var poiIDcIDName = poi.clusterName
                        commonNameMap.get(poi.clusterName) match {
                            case Some(commonName) => poiIDcIDName = commonName
                            case None             => ()
                        }

                        var keepPoi = true
                        this.boundaryPoisToKeepHMBD.value.get(poi.poiId) match {
                            case Some(pIDcIDwhoKeepsPoi) => {
                                if(poi.clusterName != pIDcIDwhoKeepsPoi)
                                    keepPoi = false
                            }
                            case None => ()
                        }

                        poi.clusterName = poiIDcIDName
                        if(keepPoi)
                            Seq((poi.clusterName, poi))
                        else
                            Seq()
                    }
                }
            }
        }


        //RDD[clusterName, HashMap[poiID, poi]]
        val dbclusterRDD = preFinalClusterRDD.aggregateByKey(HashMap[String, DBPOI]())(
            //SeqOp
            (zPoiHM, poi) => zPoiHM += ((poi.poiId, poi)),

            //CombOp
            (hm1, hm2) => hm1 ++= hm2
        )

        //RDD[(String, Array[POI])]
        dbclusterRDD.mapValues(_.values.toArray)
    }


    /*
    * This method should be called after
    * finishing using this class(e.g: writing results, or printing stats).
    * */
    def clear(): Unit ={
        this.clusterRDD.unpersist(true)
        this.boundaryPoisToKeepHMBD.destroy()
        this.mergingClusterNameVecBD.destroy()
        this.spatialPartitionerBD.destroy()
    }

}



