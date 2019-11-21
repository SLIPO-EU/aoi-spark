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
import scala.collection.mutable.{ArrayBuffer, HashMap}


class Hotspots() extends Serializable {

    protected def lonLatToCelId(lon: Double, lat: Double,
                                minLon: Double, minLat: Double,
                                gs: Double, lonBits: Int) : Long = {

        val lonCol  = math.floor((lon - minLon) / gs).toLong
        val latRow  = math.floor((lat - minLat) / gs).toLong

        (lonCol << (63 - lonBits)) | latRow
    }

    def toMask(num: Int): Long = {

        var i = 1
        var res = 1L

        while(i < num){
            res = (res << 1) + 1
            i = i + 1
        }

        res
    }


    def cellIDToPID(cellID: Long, pSizeK: Int, lonBits: Int, latBits: Int) : Long = {

        val lonCol  = cellID >> (63 - lonBits)
        val latRow  = cellID & toMask(latBits)

        val pLonCol = lonCol / pSizeK
        val pLatCol = latRow / pSizeK

        (pLonCol << (63 - lonBits)) | pLatCol
    }

    protected def cellIDToLonLat(cellID: Long,
                                 gs_cell: Double,
                                 minLon: Double, minLat: Double,
                                 lonBits: Int, latBits: Int
                                ) : (Double, Double) = {

        val lonCol  = cellID >> (63 - lonBits)
        val latRow  = cellID & toMask(latBits)

        (minLon + lonCol * gs_cell , minLat + latRow * gs_cell )
    }

    /* Trancate Big Decimal Values, for better appearance. */
    def roundAt(x: Double, p: Int): Double = {
        BigDecimal(x).setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble
    }


    def getNBCellIDs(cellID: Long,
                     gs: Double,
                     minLon: Double, minLat: Double,
                     maxLon: Double, maxLat: Double,
                     lonBits: Int, latBits: Int) : IndexedSeq[Long] = {


        val lonCol  = cellID >> (63 - lonBits)
        val latRow  = cellID & toMask(latBits)


        for{
            iLonCol <- (lonCol - 1) to (lonCol + 1)
            jLatRow <- (latRow - 1) to (latRow + 1)

            iLon = roundAt(minLon + iLonCol * gs, 6)
            jLat = roundAt(minLat + jLatRow * gs, 6)

            if(iLon >= minLon && iLon < maxLon && jLat >= minLat && jLat < maxLat)

        }yield {

            ( iLonCol << (63 - lonBits) ) | jLatRow
        }

    }


    protected def cellIDToGeometry(cellID: Long, gs_cell: Double,
                                   minLon: Double, minLat: Double,
                                   lonBits: Int, latBits: Int,
                                   geometryFactory: GeometryFactory): Geometry = {


        val (cellLon, cellLat) = cellIDToLonLat(cellID, gs_cell, minLon, minLat, lonBits, latBits)

        val cellLon_2 = roundAt(cellLon, 6)
        val cellLat_2 = roundAt(cellLat, 6)

        val rLon = cellLon_2 + gs_cell
        val uLat = cellLat_2 + gs_cell

        val coordinateArr = Array(
            new Coordinate(cellLon_2, cellLat_2),
            new Coordinate(cellLon_2, uLat),
            new Coordinate(rLon, uLat),
            new Coordinate(rLon, cellLat_2),
            new Coordinate(cellLon_2, cellLat_2)
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
                          hotSpotsArr: Array[(Long, Double)],
                          gs_cell: Double,
                          minLon: Double,
                          minLat: Double,
                          lonBits: Int,
                          latBits: Int,
                          unionCells: Boolean
                      ) : Array[(Int, Geometry, Double)] = {

        val geometryFactory = new GeometryFactory()

        if(unionCells){
            val resGeomVec = hotSpotsArr.foldLeft(Vector[(Geometry, Double, Int)]() )(
                (zVec, x) => insertXintoGeomVec((cellIDToGeometry(x._1, gs_cell, minLon, minLat, lonBits, latBits, geometryFactory), x._2), zVec)
            )

            resGeomVec.map(x => (x._1, x._2 / x._3) )
                .zipWithIndex
                .map(x => (x._2, x._1._1, x._1._2) )
                .toArray
        }
        else{

            hotSpotsArr.map(x => (cellIDToGeometry(x._1, gs_cell, minLon, minLat, lonBits, latBits, geometryFactory), x._2))
                .zipWithIndex
                .map(x => (x._2, x._1._1, x._1._2))
        }
    }


    def numToRepresentableBits(x: Int): Int = {

        var i = 0
        var temp = x.toDouble

        while(temp >= 1.0){
            temp = temp / 2.0
            i    = i + 1
        }

        i
    }


    def toBits(maxDeg: Double, minDeg: Double, gsDeg: Double) : Int = {

        numToRepresentableBits(math.ceil((maxDeg - minDeg) / gsDeg).toInt)
    }


    def hotSpots(
                    poiRDD: RDD[(String, (Double, Double, mutable.HashMap[String, Object]))],
                    score_col: String,

                    gsCell : Double,
                    pSize_k: Int,
                    top_k : Int,
                    nbCellWeight: Double,
                    unionCells: Boolean
                ) : (Array[(Int, Geometry, Double)], ArrayBuffer[String]) = {

        val logHS_ArrBuff = ArrayBuffer[String]()

        val poiRDD_2 = poiRDD.map{
            case (id, (lon, lat, hm)) => {
                if(hm("__include_poi__").asInstanceOf[Boolean])
                    (lon, lat, hm(score_col).asInstanceOf[Double])
                else
                    (lon, lat, 0.0)
            }
        }

        findHotSpots(poiRDD_2, gsCell, pSize_k, top_k, nbCellWeight, unionCells, logHS_ArrBuff)
    }


    def hotSpots(
                    poiRDD: RDD[POI],

                    gsCell : Double,
                    pSize_k: Int,
                    top_k : Int,
                    nbCellWeight: Double,
                    unionCells: Boolean
                ) : (Array[(Int, Geometry, Double)], ArrayBuffer[String]) = {

        val logHS_ArrBuff = ArrayBuffer[String]()

        val inputRDD = poiRDD.map(poi => (poi.x, poi.y, poi.score))
        findHotSpots(inputRDD, gsCell, pSize_k, top_k, nbCellWeight, unionCells, logHS_ArrBuff)
    }


    def findHotSpots(
                        //        RDD[( lon,    lat,   score)]
                        inputRDD: RDD[(Double, Double, Double)],

                        gsCell : Double,
                        pSize_k: Int,
                        top_k : Int,
                        nbCellWeight: Double,
                        unionCells: Boolean,
                        logArrBuff: ArrayBuffer[String]
                    ) : (Array[(Int, Geometry, Double)], ArrayBuffer[String]) = {

        val inputRDD_2 =  inputRDD.persist(DISK_ONLY)

        //minLon, minLat, maxLon, maxLat
        val (minLon, minLat, maxLon, maxLat) = inputRDD_2.aggregate((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))(
            (zT, x) => {
                var minLon = zT._1
                var minLat = zT._2
                var maxLon = zT._3
                var maxLat = zT._4

                if (x._1 < minLon)
                    minLon = x._1

                if (x._2 < minLat)
                    minLat = x._2

                if (x._1 > maxLon)
                    maxLon = x._1

                if (x._2 > maxLat)
                    maxLat = x._2

                (minLon, minLat, maxLon, maxLat)
            },
            (zT1, zT2) => {
                var minLon = zT1._1
                var minLat = zT1._2
                var maxLon = zT1._3
                var maxLat = zT1._4

                if(zT2._1 < minLon)
                    minLon = zT2._1

                if(zT2._2 < minLat)
                    minLat = zT2._2

                if (zT2._3 > maxLon)
                    maxLon = zT2._3

                if (zT2._4 > maxLat)
                    maxLat = zT2._4

                (minLon, minLat, maxLon, maxLat)
            }
        )

        //Calculate Bits in each Dimension
        val lonBits  = toBits(maxLon, minLon, gsCell)       //Longitude Bits
        val latBits  = toBits(maxLat, minLat, gsCell)       //Latitude  Bits

        //RDD[CellID, Score]
        val cellRDD_1 = inputRDD_2.map{
            case (lon, lat, score) => (lonLatToCelId(lon, lat, minLon, minLat, gsCell, lonBits), score)
        }

        //RDD[CellID, Score]
        val cellRDD_2 = cellRDD_1.reduceByKey(_ + _).persist(MEMORY_AND_DISK)

        val (totalNumOfCells, totalScoreSum, totalScoreSumPow2) = cellRDD_2.aggregate(0, 0.0, 0.0)(
            (z, x)   => (z._1 + 1, z._2 + x._2, z._3 + (x._2 * x._2)),
            (s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3)
        )

        logArrBuff += s"totalNumOfCells = $totalNumOfCells"
        logArrBuff += s"totalScoreSum = $totalScoreSum"
        logArrBuff += s"totalScoreSum^2 = $totalScoreSumPow2"

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
                val origPID = cellIDToPID(cellID, pSize_k, lonBits, latBits)

                //Get the Neighbourhood of the Cell.
                val cellID_NBSeq = getNBCellIDs(cellID, gsCell, minLon, minLat, maxLon, maxLat, lonBits, latBits)

                //All Partition Ids of All cells inside Neighbourhood.
                val pIDSeq = cellID_NBSeq.map(cellID_i =>  cellIDToPID(cellID_i, pSize_k, lonBits, latBits) )

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
        val partitionRDD_2 = partitionRDD_1.aggregateByKey(HashMap[Long, (Double, Boolean)]() )(
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
                    val cellIDNBSeq = getNBCellIDs(cellID, gsCell, minLon, minLat, maxLon, maxLat, lonBits, latBits).filter(cID => cID != cellID)

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


        implicit val sortByMaxGi = new Ordering[(Long, Double)] {

            override def compare(a: (Long, Double), b: (Long, Double) ) = {

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

        (toGeomScoreArr(top_k_GiArr, gsCell, minLon, minLat, lonBits, latBits, unionCells), logArrBuff)
    }
}

