package HotSpots

/*
* HotSpot Detection
* Distributed Edition in Spark & Scala.
*
* Authors: Panagiotis Kalampokis, Dr. Dimitris Skoutas
* */

import mySparkSession.mySparkSession
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import Spatial._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._

import scala.collection.mutable
import scala.collection.mutable.HashMap


class Hotspots() extends Serializable {

    protected def lonLatToCelId(lon: Double, lat: Double,
                                startX: Double, startY: Double,
                                gs: Double) : String = {

        val lonCol  = math.floor((lon - startX) / gs).toLong
        val latRow  = math.floor((lat - startY) / gs).toLong

        lonCol + "c" + latRow
    }


    def cellIDToPID(cellID: String, pSizeK: Int) : String = {

        val lonLatCols = cellID.split("c")

        val pLonCol = lonLatCols(0).toLong / pSizeK
        val pLatCol = lonLatCols(1).toLong / pSizeK

        pLonCol + "p" + pLatCol
    }

    protected def cellIDToLonLat(cellID: String,
                                 gs_cell: Double,
                                 startX: Double, startY: Double) : (Double, Double) = {

        val lonLatArr = cellID.split("c")
        val lonCol = lonLatArr(0).toLong
        val latRow = lonLatArr(1).toLong

        (startX + lonCol * gs_cell , startY + latRow * gs_cell )
    }


    protected def getNBCellIDs(cellID: String) : IndexedSeq[String] = {

        val lonLatArr = cellID.split("c")
        val lonCol = lonLatArr(0).toLong
        val latRow = lonLatArr(1).toLong

        for{
            iLonCol <- (lonCol - 1) to (lonCol + 1)
            jLatRow <- (latRow - 1) to (latRow + 1)
        }yield {
            iLonCol + "c" + jLatRow
        }
    }

    protected def cellIDToGeometry(cellID: String, gs_cell: Double,
                                   startX: Double, startY: Double,
                                   geometryFactory: GeometryFactory): Geometry = {


        val (cellLon, cellLat) = cellIDToLonLat(cellID, gs_cell, startX, startY)

        val rLon = cellLon + gs_cell
        val uLat = cellLat + gs_cell

        val coordinateArr = Array(
            new Coordinate(cellLon, cellLat),
            new Coordinate(cellLon, uLat),
            new Coordinate(rLon, uLat),
            new Coordinate(rLon, cellLat),
            new Coordinate(cellLon, cellLat)
        )

        geometryFactory.createPolygon(coordinateArr).asInstanceOf[Geometry]
    }

    /*
    * Returns a Vector[Geometry, TotalScore, counter]
    * */
    protected def insertXintoGeomVec(x: (Geometry, Double) , zVec: Vector[(Geometry, Double, Int)] ) : Vector[(Geometry, Double, Int)] = {

        var i = 0
        var unionedGeom = x._1
        var score = x._2
        var count = 1
        var tmpVec = Vector[(Geometry, Double, Int)]()

        while(i < zVec.size){
            val (i_geom, i_score, i_count) = zVec(i)

            if(unionedGeom.intersects( i_geom )) {
                unionedGeom = unionedGeom.union(i_geom)
                score = score + i_score
                count = count + i_count
            }
            else
                tmpVec = tmpVec :+ zVec(i)
            i = i + 1
        }

        (unionedGeom, score, count) +: tmpVec
    }


    def toGeomScoreArr(
                        hotSpotsArr: Array[(String, Double)],
                        gs_cell: Double,
                        startX: Double,
                        startY: Double,
                        unionCells: Boolean
                      ) : Array[(Int, Geometry, Double)] = {

        val geometryFactory = new GeometryFactory()

        if(unionCells){
            val resGeomVec = hotSpotsArr.foldLeft(Vector[(Geometry, Double, Int)]() )(
                (zVec, x) => insertXintoGeomVec((cellIDToGeometry(x._1, gs_cell, startX, startY, geometryFactory), x._2), zVec)
            )

            resGeomVec.map(x => (x._1, x._2 / x._3) )
                      .zipWithIndex
                      .map(x => (x._2, x._1._1, x._1._2) )
                      .toArray
        }
        else{

            hotSpotsArr.map(x => (cellIDToGeometry(x._1, gs_cell, startX, startY, geometryFactory), x._2))
                       .zipWithIndex
                       .map(x => (x._2, x._1._1, x._1._2))
        }
    }


    def hotSpots(
                  poiRDD: RDD[(String, (Double, Double, mutable.HashMap[String, Object]))],
                  score_col: String,

                  gsCell : Double,
                  pSize_k: Int,
                  top_k : Int,
                  nbCellWeight: Double,
                  unionCells: Boolean
                ) : Array[(Int, Geometry, Double)] = {


        val poiRDD_2 = poiRDD.map{
            case (id, (lon, lat, hm)) => {
                if(hm("__include_poi__").asInstanceOf[Boolean])
                    (lon, lat, hm(score_col).asInstanceOf[Double])
                else
                    (lon, lat, 0.0)
            }
        }

        findHotSpots(poiRDD_2, gsCell, pSize_k, top_k, nbCellWeight, unionCells)
    }


    def hotSpots(
                 poiRDD: RDD[POI],

                 gsCell : Double,
                 pSize_k: Int,
                 top_k : Int,
                 nbCellWeight: Double,
                 unionCells: Boolean) : Array[(Int, Geometry, Double)] = {

        val inputRDD = poiRDD.map(poi => (poi.x, poi.y, poi.score))
        findHotSpots(inputRDD, gsCell, pSize_k, top_k, nbCellWeight, unionCells)
    }


    def findHotSpots(
                    //        RDD[( lon,    lat,   score)]
                    inputRDD: RDD[(Double, Double, Double)],

                    gsCell : Double,
                    pSize_k: Int,
                    top_k : Int,
                    nbCellWeight: Double,
                    unionCells: Boolean) : Array[(Int, Geometry, Double)] = {

        //Take the 1st poi as the starting point of X,Y axis.
        val (startX, startY, _) = inputRDD.take(1).head

        //RDD[CellID, Score]
        val cellRDD_1 = inputRDD.map{
            case (lon, lat, score) => (lonLatToCelId(lon, lat, startX, startY, gsCell), score)
        }

        //RDD[CellID, Score]
        val cellRDD_2 = cellRDD_1.reduceByKey(_ + _).persist(MEMORY_AND_DISK)

        val (totalNumOfCells, totalScoreSum, totalScoreSumPow2) = cellRDD_2.aggregate(0, 0.0, 0.0)(
            (z, x)   => (z._1 + 1, z._2 + x._2, z._3 + (x._2 * x._2)),
            (s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3)
        )

        val xMeanBD =  mySparkSession.sparkContext.broadcast(totalScoreSum / totalNumOfCells.toDouble)
        val sMeanBD = mySparkSession.sparkContext.broadcast(math.sqrt( (totalScoreSumPow2 / totalNumOfCells.toDouble) - xMeanBD.value * xMeanBD.value))

        /*
        * We Copy every Cell to all possible neigbours internally.
        * We map each fresh new copy to a PartitionID.
        * We keep Only those who map to different Partition. (Up to 4 Copies Maximun for each cell)
        */
        //RDD[partitionID, (CellID, score, isReal)]
        val partitionRDD_1 = cellRDD_2.flatMap{
            case (cellID, score) => {

                //What is the PartitionID of the current Cell
                //CellId -> PID
                val origPID = cellIDToPID(cellID, pSize_k)

                //Get the Neighbourhood of the Cell.
                val cellID_NBSeq = getNBCellIDs(cellID)

                //All Partition Ids of All cells inside Neighbourhood.
                val pIDSeq = cellID_NBSeq.map(cellID_i =>  cellIDToPID(cellID_i, pSize_k) )

                pIDSeq.distinct.map{
                    i_PID => {

                        if(i_PID == origPID)
                            (i_PID, (cellID, score, true) )
                        else
                            (i_PID, (cellID, score, false) )
                    }
                }
            }
        }

        /*
        * Here We aggregate Per Partition.
        */
        //RDD[partitionRDD, HashMap[CellID, (score, isReal)] ]
        val partitionRDD_2 = partitionRDD_1.aggregateByKey(HashMap[String, (Double, Boolean)]() )(
            //SeqOp
            (hm , x) => {
                hm.update(x._1, (x._2, x._3) )
                hm
            },
            //Comb Op
            (hm1, hm2) => {
                hm1 ++= hm2
            }
        )

        var top_k_hotSpots = top_k

        //If User has specified to retrieve all hotSpots
        if(top_k_hotSpots <= 0)
            top_k_hotSpots = totalNumOfCells

        //RDD[CellID, Gi*]
        val giStarRDD = partitionRDD_2.flatMap{

            case (pID, hashMap) => {

                var sumWijXj   = 0.0
                var sumWij     = 0.0
                var sumWijP2   = 0.0
                for{
                    (cellID, (score, isReal)) <- hashMap

                    if(isReal)

                } yield{
                    sumWijXj = score   //For the cell in question wij is exclusively 1! The purpose is to consider with different weight your neighbours from yourself!
                    sumWij   = 1.0     //wi,1 = 1.0
                    sumWijP2 = 1.0
                    val cellIDNBSeq = getNBCellIDs(cellID).filter(cID => cID != cellID)

                    cellIDNBSeq.foreach{
                        cell_i => {
                            hashMap.get(cell_i) match {
                                case Some(xi) => {
                                    sumWijXj = sumWijXj + nbCellWeight * xi._1  //Sum(WijXj) all the cells in the Neighborhood
                                    sumWij   = sumWij   + nbCellWeight          //Sum(Wij) all the cells in the Neighborhood
                                    sumWijP2 = sumWijP2 + nbCellWeight * nbCellWeight
                                }
                                case None     => ()
                            }
                        }
                    }

                    val numerator   = sumWijXj - xMeanBD.value * sumWij
                    val denominator = sMeanBD.value * math.sqrt((totalNumOfCells * sumWijP2 - sumWij * sumWij) / (totalNumOfCells - 1) )
                    val gi = numerator / denominator

                    (cellID, gi)
                }
            }
        }


        implicit val sortByMaxGi = new Ordering[(String, Double)] {

            override def compare(a: (String, Double), b: (String, Double) ) = {

                if(a._2 > b._2)
                    +1
                else if(a._2 < b._2)
                    -1
                else
                     0
            }
        }

        //Array[(cellID, Score)]
        val top_k_GiArr = giStarRDD.top(top_k_hotSpots)(sortByMaxGi)

        cellRDD_2.unpersist()
        xMeanBD.destroy()
        sMeanBD.destroy()

        toGeomScoreArr(top_k_GiArr, gsCell, startX, startY, unionCells)
    }
}
