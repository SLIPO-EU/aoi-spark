package DBSCAN


import java.util

import MySparkContext.mySparkContext
import Poi.{BoundaryPOI, POI_4}
import com.vividsolutions.jts.geom.GeometryFactory
import gr.athenarc.imsi.slipo.analytics.POI
import gr.athenarc.imsi.slipo.analytics.clustering.DBSCANClusterer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



class DBSCAN_Clusterer() extends Serializable {


    private var bdBoundaryPoisNotDenseMap: Broadcast[scala.collection.Map[String, Set[String]]] = null
    private var connectionSetBD: Broadcast[Vector[Set[String]]] = null



    protected def lonLatToCelId(lon: Double, lat: Double,
                                minLon: Double, minLat: Double,
                                psLonLat: Double) : String = {


        val lonCol  = ((lon - minLon) / psLonLat).toLong
        val latRow  = ((lat - minLat) / psLonLat).toLong


        lonCol.toString + "c" + latRow.toString

    }


    protected def isValidPoint(lon   : Double,
                               lat   : Double,
                               minLon: Double,
                               maxLon: Double,
                               minLat: Double,
                               maxLat: Double
                              ) : Boolean = {


        if(lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat)
            true
        else
            false

    }


    protected def getAllPossibleNBPartitionsInRadiusEps(
                                                       lon   : Double,
                                                       lat   : Double,

                                                       minLon: Double,
                                                       maxLon: Double,
                                                       minLat: Double,
                                                       maxLat: Double,

                                                       psLonLat : Double,

                                                       eps: Double

                                                       ) : Set[String] = {



        val lLon = lon - eps - eps / 10.0
        val rLon = lon + eps + eps / 10.0

        val dLat = lat - eps - eps / 10.0
        val uLat = lat + eps + eps / 10.0


        var uniquePIDSet = Set[String]()


        if(isValidPoint(lLon, dLat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(lLon, dLat, minLon, minLat, psLonLat)

        if(isValidPoint(lon, dLat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(lon , dLat, minLon, minLat, psLonLat)

        if(isValidPoint(rLon, dLat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(rLon, dLat, minLon, minLat, psLonLat)



        if(isValidPoint(lLon, lat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(lLon, lat, minLon, minLat, psLonLat)

        if(isValidPoint(lon, lat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(lon , lat, minLon, minLat, psLonLat)

        if(isValidPoint(rLon, lat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(rLon, lat, minLon, minLat, psLonLat)



        if(isValidPoint(lLon, uLat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(lLon, uLat, minLon, minLat, psLonLat)

        if(isValidPoint(lon, uLat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(lon , uLat, minLon, minLat, psLonLat)

        if(isValidPoint(rLon, uLat, minLon, maxLon, minLat, maxLat))
            uniquePIDSet += lonLatToCelId(rLon, uLat, minLon, minLat, psLonLat)


        uniquePIDSet

    }



    /*
    * Maps Data to Pois.
    * Partitions Pois according to a Grid of Size gsLon, gsLat = lambda *  epsilon.     each Cell.
    * Pois that fall near eps_boundary_dist from Boundaries, are copied to nearby partitions(Cells).
    *
    * Returns RDD[PartitionID, poiID ]
    */
    def gridPartition(
                     //      RDD[(poiID,   lon,    lat  )]
                     poiRDD: RDD[(String, Double, Double)],

                     epsilon : Double,
                     lambda  : Int,


                     minLon: Double,
                     maxLon: Double,
                     minLat: Double,
                     maxLat: Double,

                                                       //RDD[PartitionID, poiID ]
                     geometryFactory: GeometryFactory) : RDD[(String,     POI)] = {


        poiRDD.flatMap{

            case (pID, lon, lat) => {


                if ( (lon >= minLon && lon <= maxLon) && (lat >= minLat && lat <= maxLat) ) {

                    val origCellID = lonLatToCelId(lon,
                        lat,
                        minLon,
                        minLat,
                        epsilon * lambda
                    )


                    val nbCellIDs = getAllPossibleNBPartitionsInRadiusEps(
                        lon,
                        lat,
                        minLon,
                        maxLon,
                        minLat,
                        maxLat,
                        epsilon * lambda,
                        epsilon
                    )


                    nbCellIDs.map {

                        cellID => {

                            val poi = new POI(pID, "", lon, lat, new util.ArrayList[String](), 1.0, geometryFactory)

                            if (nbCellIDs.size > 1)
                                poi.getAttributes.put("BoundaryPoint", "BoundaryPoint")


                            (cellID, poi)

                        }
                    }

                }
                else{

                    Set.empty[(String, POI)]
                }
            }
        }

    }



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




    /*
    * Takes an RDD[partitionID, ArrayList[POI]],
    *
    * Performs DBSCAN in Each partition Seperately
    * and Returns an RDD[clusterID, HashMap[poiID, poi]]
    * */
    def dbscan(
              //      RDD[(poiID,   lon,    lat  )]
              poiRDD: RDD[(String, Double, Double)],

              epsilon : Double,
              lambda  : Int,
              minPts  : Int,


              minLon: Double,
              maxLon: Double,
              minLat: Double,
              maxLat: Double,

              geometryFactory: GeometryFactory

              ) = {


        var startTime = System.nanoTime()

        //RDD[pID, poiID]
        val partitionedRDD = gridPartition(poiRDD, epsilon, lambda, minLon, maxLon, minLat, maxLat, geometryFactory)



        //RDD[pID, ArrayList[Poi]]
        val partitionRDD = partitionedRDD.aggregateByKey(new util.ArrayList[POI]())(
            //SeqOp
            (poiArrLst, poi) => {
                poiArrLst.add(poi)
                poiArrLst
            },

            //CombOp
            (poiArrLst_1, poiArrLst_2) => {
                poiArrLst_1.addAll(poiArrLst_2)
                poiArrLst_1
            }
        )


        println("partitionRDD Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        startTime = System.nanoTime()

        //RDD[(String, (ArrayBuffer[ArrayBuffer[POI_4]], ArrayBuffer[(String, BoundaryPOI)]))]
        //RDD[pID,     (ArrayBuffer[ArrayBuffer[POI]]  , ArrayBuffer[(poiID, BoundaryPOI)] ) ]
        val partitionClusterRDD = partitionRDD.mapPartitions(

            pID_poiArrListIter => {

                val clusterer = new DBSCANClusterer(epsilon, minPts)


                for((pID, poiArrList) <- pID_poiArrListIter) yield {

                    //List[POI]  -->  List[List[POI]]
                    val clusterPoiList = clusterer.cluster(poiArrList)


                    val clusterArrBuff  = ArrayBuffer[ArrayBuffer[POI_4]]()

                    //                       ArrayBuffer[(pID, BoundaryPOI)]()
                    val boundaryPoiArrBuff = ArrayBuffer[(String, BoundaryPOI)]()


                    var i = 0
                    var j = 0

                    var cluster_i: util.List[POI] = null
                    var poi_j: POI = null
                    var cluster_i_size = 0
                    var pID_cID = ""

                    var isBoundaryPoi = false
                    var isDensePoi    = false

                    while(i < clusterPoiList.size()){

                        clusterArrBuff.append(ArrayBuffer[POI_4]())
                        cluster_i = clusterPoiList.get(i)
                        cluster_i_size = cluster_i.size()

                        j = 0

                        while(j < cluster_i_size){

                            poi_j = cluster_i.get(j)

                            pID_cID = pID + "a" + i

                            isDensePoi = poi_j.getAttributes.containsValue("DENSE")
                            isBoundaryPoi = poi_j.getAttributes.containsValue("BoundaryPoint")


                            clusterArrBuff(i).append( POI_4(pID_cID, poi_j.getId, isDensePoi , isBoundaryPoi ) )


                            if(isBoundaryPoi)
                                boundaryPoiArrBuff.append( (poi_j.getId, BoundaryPOI(pID_cID, isDensePoi) ) )


                            j = j + 1
                        }


                        i = i + 1
                    }


                    (pID, (clusterArrBuff, boundaryPoiArrBuff))

                }

            },

            true
        )
        .persist(MEMORY_AND_DISK_SER)


        println("Find Clusters in Partitions Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        startTime = System.nanoTime()


        //RDD[(poiID, BoundaryPOI)]
        val boundaryPoiRDD = partitionClusterRDD.map{
                                 case (pID, (clusterArrBuff, boundaryPoisArrBuff) ) => boundaryPoisArrBuff
                             }
                             .flatMap(x => x)                                           



        //RDD[poi_ID, Set[pID_cID, isDense?] ]
        val boundaryPoiRDD_2 = boundaryPoiRDD.aggregateByKey(Set[(String, Boolean)]())(
            //SeqOp
            (zSet, boundaryPoi) => zSet + ( (boundaryPoi.pID_cID, boundaryPoi.isDense) ) ,

            //CombOp
            (set1, set2) => set1 ++ set2

        ).persist(MEMORY_AND_DISK_SER)


        println("RDD[BoundaryPoiID, Set[pIDcID, isDense]] Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        startTime = System.nanoTime()


        //All Boundary pois that are NOT DENSE in ALL clusters.
        //That means they are Boundaries in all clusters. So Every poi will be assigned to 1 and only 1 cluster (randomly).
        //RDD[poiID, Set[pID_cID]]
        val boundaryPoisNotDenseRDD = boundaryPoiRDD_2.filter{
            case (poi_ID, set) => !(set.foldLeft(false)((zFlag, x) => zFlag | x._2 ))
        }.mapValues{
            set => set.map(_._1)
        }


        //All Boundary pois that are DENSE in at Least 1 cluster. That means MERGE.
        val boundaryPoiRDD_4 = boundaryPoiRDD_2.filter{
            case (poi_ID, set) => set.exists(x => x._2)
        }



        //RDD[Set[String]]
        val boundaryPoiRDD_5 = boundaryPoiRDD_4.map{
            case (poi_ID, set) => set.foldLeft(Set[String]())( (zSet, x) => zSet + x._1)
        }



        startTime = System.nanoTime()

        //These clusters should be Merged, because we Found a DENSE + BOUNDARY poi in at least one cluster.
        //Vector[Set[String]]
        //TreeAggregate 
        val aggregatedPoiVec_6 = boundaryPoiRDD_5.treeAggregate(Vector[Set[String]]())(
            //SeqOP
            (zVec, xSet) => insertSetIntoVec(zVec, xSet),

            //CombOp
            (vec1, vec2) => vec2.foldLeft(vec1)( (zVec, xSet) => insertSetIntoVec(zVec, xSet))
        )


        println("Vector[Set[pIDcID]] Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")

        startTime = System.nanoTime()


        //We Broadcast the name of clusters(pID_cID) that should be Merged.
        this.connectionSetBD  = mySparkContext.sc.broadcast(aggregatedPoiVec_6)


        //We also broadcast the name of clusters(pID_cID) that should NOT be Merged
        // along with the Poi_Id that was found same in multiple clusters.(NOT DENSE) poi in all clusters.

        this.bdBoundaryPoisNotDenseMap = mySparkContext.sc.broadcast(boundaryPoisNotDenseRDD.collectAsMap())

        val preFinalRDD = partitionClusterRDD.mapPartitions{

            pIDclusterListIter => {

                //Make a Connection Map Set[pID_cID] --> CommonName
                val connectionsVec = this.connectionSetBD.value.map{
                    set => {
                        val name = set.toVector.sortWith(_ < _).mkString("b")
                        (set, name)
                    }
                }


                //Map[poi_ID, Set[String]]
                val boundaryNotDenseMap = this.bdBoundaryPoisNotDenseMap.value

                //ArrayBuffer[clusterName, poi]
                val tmpArrBuff = ArrayBuffer[(String, POI_4)]()

                for((pID, (clusterList, boundaryPois)) <- pIDclusterListIter ) {

                    //cluster_id
                    var i = 0
                    var j = 0
                    var clusterName = ""
                    val totalClusters = clusterList.size
                    var keepPoiFlag = true

                    while (i < totalClusters) {

                        val cluster_i = clusterList(i)
                        val cluster_i_size = cluster_i.size

                        j = 0
                        while (j < cluster_i_size) {

                            val poi = cluster_i(j)


                            clusterName = pID + "a" + i


                            boundaryNotDenseMap.get(poi.poiID) match {
                                case Some(set) => {
                                    if(set.head == clusterName)
                                        keepPoiFlag = true
                                    else
                                        keepPoiFlag = false
                                }

                                case None => keepPoiFlag = true
                            }


                            val vecTupleOpt = connectionsVec.find{
                                case (set, newName) => set.exists(x => x == clusterName)
                            }

                            vecTupleOpt match {
                                case Some((set, newName)) => clusterName = newName
                                case None => ()
                            }


                            if(keepPoiFlag)
                                tmpArrBuff.append((clusterName, poi))


                            j = j + 1
                        }

                        i = i + 1
                    }

                }


                tmpArrBuff.toIterator

            }
        }


        //RDD[clusterName, HashMap[poiID, poi]]
        val finalRDD = preFinalRDD.aggregateByKey(mutable.Set[String]())(
            //SeqOp
            (zPoiSet, poi) => {
                zPoiSet += poi.poiID
            },

            //CombOp
            (zSet1, zSet2) => zSet1 ++= zSet2
        )


        println("Final Time = " + (System.nanoTime() - startTime) / 1000000000L + " sec")



        partitionClusterRDD.unpersist()
        boundaryPoiRDD_2.unpersist()


        finalRDD


    }






    def clusterToStr(cluster: mutable.HashMap[String, POI]): String = {

        var s = "MULTIPOINT ("

        val vecCluster = cluster.map(_._2).toVector

        val size = vecCluster.size


        for(i <- 0 until (size - 1) ){

            s = s + s"(${vecCluster(i).getPoint.getX} ${vecCluster(i).getPoint.getY}), "
        }

        s = s + s"(${vecCluster.last.getPoint.getX} ${vecCluster.last.getPoint.getY}))"


        s
    }



    def writeResultsToFile(finalRDD: RDD[(String, mutable.HashMap[String, POI])], outputFile: String) : Unit = {

        finalRDD.map{
            case (clusterName, cluster) => {
                clusterName + ";" + this.clusterToStr(cluster) + ";" + cluster.size
            }
        }
        .saveAsTextFile(outputFile)

    }



    def clean_BD_VArs() = {

        this.connectionSetBD.destroy()
        this.bdBoundaryPoisNotDenseMap.destroy()

    }


}


