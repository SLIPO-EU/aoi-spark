package HotSpots

/*
* HotSpot Detection in 3D Space (longitude, latitude, time) by Getis Ord G*
* https://pro.arcgis.com/en/pro-app/tool-reference/spatial-statistics/h-how-hot-spot-analysis-getis-ord-gi-spatial-stati.htm
* Distributed Edition in Spark & Scala.
*
* Head of project: Dimitris Skoutas
* Coded By: Panagiotis Kalampokis
* */

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel._
import scala.collection.mutable.{ArrayBuffer, HashMap}


class Hotspots_3D() extends Serializable {


    protected def lonLatTimeToCelId(lon: Double, lat: Double, t: Long,
                                    minLon: Double, minLat: Double, minT: Long,
                                    maxLon: Double, maxLat: Double, maxT: Long,
                                    gsLon: Double, gsLat: Double, gt: Long,
                                    lonBits: Int, latBits: Int) : Array[Int] = {

        val lonCol  = ((lon - minLon) / gsLon).toInt
        val latRow  = ((lat - minLat) / gsLat).toInt
        val tCol    = ((t - minT).toDouble / gt.toDouble).toInt

        var lonArr = Array[Int]()
        if(lon < maxLon)
            lonArr = lonArr :+ lonCol

        //If touches Lon axis
        if(((lon - minLon) % gsLon) == 0 && (lonCol-1) >= 0){
            lonArr = lonArr :+ (lonCol - 1)
        }

        var latArr = Array[Int]()
        if(lat < maxLat)
            latArr = latArr :+ latRow

        //If touches Lat axis
        if(((lat - minLat) % gsLat) == 0  && (latRow-1) >= 0){
            latArr = latArr :+ (latRow - 1)
        }

        var tArr = Array[Int]()
        if(t < maxT)
            tArr = tArr :+ tCol

        //If touches Time axis
        if(((t - minT) % gt) == 0  && (tCol-1) >= 0){
            tArr = tArr :+ (tCol - 1)
        }

        for{
            lonCol_i <- lonArr
            latRow_i <- latArr
            tCol_i   <- tArr
        } yield {
            val cellid_i = (lonCol_i << (32 - lonBits)) | (latRow_i << (32 - lonBits - latBits)) | tCol_i
            cellid_i
        }

    }

    def toMask(num: Int): Int = {

        var i = 1
        var res = 1L

        while(i < num){
            res = (res << 1) + 1
            i = i + 1
        }

        res.toInt
    }


    def cellIDToPID(cellID: Int,
                    pSize_gs_K: Int,
                    pTW: Int,
                    lonBits: Int,
                    latBits: Int,
                    lonBitMask: Int,
                    latBitMask: Int,
                    tBitMask: Int
                   ) : Int = {

        val lonCol  = (cellID  >> (32 - lonBits)) & lonBitMask
        val latRow  = (cellID >> (32 - lonBits - latBits)) & latBitMask
        val tCol    = cellID & tBitMask

        val pLonCol = lonCol / pSize_gs_K
        val pLatRow = latRow / pSize_gs_K
        val pTCol   = tCol   / pTW

        (pLonCol << (32 - lonBits)) | (pLatRow << (32 - lonBits - latBits)) | pTCol
    }

    protected def cellIDToLonLatT(cellID: Int,
                                  gsLon: Double, gsLat: Double, gt_cell: Long,
                                  minLon: Double, minLat: Double, minT: Long,
                                  lonBits: Int, latBits: Int, tBits: Int
                                 ) : (Double, Double, Long) = {

        val lonCol  = (cellID  >> (32 - lonBits)) & toMask(lonBits)
        val latRow  = (cellID >> (32 - lonBits - latBits)) & toMask(latBits)
        val tCol    = cellID & toMask(tBits)

        (minLon + lonCol * gsLon , minLat + latRow * gsLat, minT + tCol * gt_cell)
    }

    /* Trancate Big Decimal Values, for better appearance. */
    def roundAt(x: Double, p: Int): Double = {
        BigDecimal(x).setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble
    }


    def getNBCellIDs(cellID: Int,
                     gsLon: Double,
                     gsLat: Double,
                     gT: Long,
                     nbSize: Int,
                     minLon: Double,
                     minLat: Double,
                     maxLon: Double,
                     maxLat: Double,
                     minT: Long,
                     maxT: Long,
                     lonBits: Int,
                     latBits: Int,
                     lonBitMask: Int,
                     latBitMask: Int,
                     tBitMask: Int
                    ) : IndexedSeq[Int] = {

        val lonCol  = (cellID  >> (32 - lonBits)) & lonBitMask
        val latRow  = (cellID >> (32 - lonBits - latBits)) & latBitMask
        val tCol    = cellID & tBitMask

        for{
            iLonCol <- (lonCol - nbSize) to (lonCol + nbSize)
            jLatRow <- (latRow - nbSize) to (latRow + nbSize)
            kTCol   <- (tCol   - nbSize) to (tCol   + nbSize)

            iLon = minLon + iLonCol * gsLon
            jLat = minLat + jLatRow * gsLat
            kT   = minT + kTCol * gT

            if(iLon >= minLon && iLon < maxLon &&
                jLat >= minLat && jLat < maxLat &&
                kT   >= minT && kT < maxT)

        }yield {
            (iLonCol << (32 - lonBits)) | (jLatRow << (32 - lonBits - latBits)) | kTCol
        }

    }


    def lowerLeftCornerLonLatToGeometry(cell_lon: Double,
                                        cell_lat: Double,
                                        gsLon: Double,
                                        gsLat: Double,
                                        geometryFactory: GeometryFactory
                                       ) = {

        val cellLon_2 = roundAt(cell_lon, 6)
        val cellLat_2 = roundAt(cell_lat, 6)

        val rLon = roundAt(cellLon_2 + gsLon, 6)
        val uLat = roundAt(cellLat_2 + gsLat, 6)

        val coordinateArr = Array(
            new Coordinate(cellLon_2, cellLat_2),
            new Coordinate(cellLon_2, uLat),
            new Coordinate(rLon, uLat),
            new Coordinate(rLon, cellLat_2),
            new Coordinate(cellLon_2, cellLat_2)
        )

        coordinateArr
        //geometryFactory.createPolygon(coordinateArr).asInstanceOf[Geometry]
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


    /**
     * HotSpot Detection in 3D Space (longitude, latitude, time) by Getis Ord G*
     * @param inputRDD An RDD[longitude, latitude, time(ms), score(Int)].
     * @param gsLon Cell longitude size in Degrees.
     * @param gsLat Cell latitude size in Degrees.
     * @param gT Time Window in milliseconds(e.g 1 hour = 60 * 60 * 1000 msec = 3600000L)
     * @param nbSize Size of the neighbourhood to consider
     * <br><br>
     * Also takes as input a boundary Box in EPSG:4326, wgs84<br>
     * @return An Array with the top-k hotCells of Gi* in wgs84, along with t(milisecs) denoting time window & Array with useful statistics.
     * */
    def findHotSpots(
                        //        RDD[( lon,    lat,   timeStamp(milisec),  score)] in WGS84
                        inputRDD: RDD[(Double, Double,     Long,        Int)],

                        gsLon : Double,                //meters or degrees
                        gsLat : Double,                //meters or degrees
                        gT     : Long = 3600000L,      //gT Time window in milisec (e.g 1 hour = 60 * 60 * 1000 milisec)

                        top_k : Int   = 50,            //Top K Hotspots
                        nbSize: Int   = 1,             //Size of the neighbourhood

                        pSize_gs_k: Int = 20,
                        pSize_ts_k : Int = 20,

                        //Bounding Box in Greece in source_crs(epsg:4326, wgs84)
                        minLon: Double = 19.57,
                        minLat: Double = 34.88,
                        maxLon: Double = 28.3,
                        maxLat: Double = 41.75,
                        //Time in msec
                        minTime: Long,
                        maxTime: Long
                    )(implicit spark: SparkSession) : (Array[(Int, Geometry, Long, Double)], ArrayBuffer[String]) = {


        val logArrBuff = ArrayBuffer[String]()


        //Calculate Bits in each Dimension
        val lonBits  = toBits(maxLon, minLon, gsLon) //Longitude Bits
        val latBits  = toBits(maxLat, minLat, gsLat) //Latitude  Bits
        val timeBits = toBits(maxTime, minTime, gT.toDouble) //maxTime, minTime in Long(msec). gT Time window in msec (e.g 1 hour = 60 * 60 * 1000 msec)

        logArrBuff += s"lonBits: $lonBits"
        logArrBuff += s"latBits: $latBits"
        logArrBuff += s"tBits: $timeBits"


        if (lonBits + latBits + timeBits > 32) {
            logArrBuff += "Bits > 32! Please Retry with lower analysis."
            println("Bits > 32! Please Retry with lower analysis.")
            return (Array[(Int, Geometry, Long, Double)](), logArrBuff)
        }


        //Broadcast Variables
        val bboxBD = spark.sparkContext.broadcast((minLon, minLat, maxLon, maxLat))
        val twBD   = spark.sparkContext.broadcast((minTime, maxTime))
        val gTBD   = spark.sparkContext.broadcast(gT)
        val gsLonBD = spark.sparkContext.broadcast(gsLon)
        val gsLatBD = spark.sparkContext.broadcast(gsLat)
        val nbSizeBD = spark.sparkContext.broadcast(nbSize)
        val lonBitsBD = spark.sparkContext.broadcast(lonBits)
        val latBitsBD = spark.sparkContext.broadcast(latBits)
        val timeBitsBD = spark.sparkContext.broadcast(timeBits)
        val pSizeGSKBD = spark.sparkContext.broadcast(pSize_gs_k)
        val pSizeGTBD   = spark.sparkContext.broadcast(pSize_ts_k)
        val lonBitMaskBD = spark.sparkContext.broadcast(toMask(lonBits))
        val latBitMaskBD = spark.sparkContext.broadcast(toMask(latBits))
        val tBitMaskBD = spark.sparkContext.broadcast(toMask(timeBits))


        //RDD[cellID, score]
        val cellRDD = inputRDD.mapPartitions {
            tupleIter => {

                val finalArrBuff = ArrayBuffer[(Int, Int)]()

                val (minLon, minLat, maxLon, maxLat) = bboxBD.value
                val (minTime, maxTime) = twBD.value
                val gsLon = gsLonBD.value
                val gsLat = gsLatBD.value
                val gT = gTBD.value
                val lonBits = lonBitsBD.value
                val latBits = latBitsBD.value

                while (tupleIter.hasNext) {
                    val (lon, lat, t, score) = tupleIter.next()

                    try {

                        if (lon >= minLon && lon <= maxLon &&
                            lat >= minLat && lat <= maxLat &&
                            t >= minTime && t <= maxTime
                        ) {
                            val cellIDArr = lonLatTimeToCelId(lon, lat, t, minLon, minLat, minTime, maxLon, maxLat, maxTime, gsLon, gsLat, gT, lonBits, latBits)
                            finalArrBuff ++= cellIDArr.map(cellID => (cellID, score))
                        }
                    }
                    catch {
                        case e: Exception =>
                    }

                }

                finalArrBuff.toIterator
            }
        }
            .reduceByKey(_ + _)
            .persist(MEMORY_AND_DISK)


        val (totalNumOfCells, totalScoreSum, totalScoreSumPow2) = cellRDD.aggregate(0, 0.0, 0.0)(
            (z, x) => (z._1 + 1, z._2 + x._2, z._3 + (x._2 * x._2)),
            (s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3)
        )

        logArrBuff += s"totalNumOfCells = $totalNumOfCells"
        logArrBuff += s"totalScoreSum = $totalScoreSum"
        logArrBuff += s"totalScoreSum^2 = $totalScoreSumPow2"

        val xMeanBD = spark.sparkContext.broadcast(totalScoreSum / totalNumOfCells.toDouble)
        val sMeanBD = spark.sparkContext.broadcast(math.sqrt((totalScoreSumPow2 / totalNumOfCells.toDouble) - xMeanBD.value * xMeanBD.value))
        val totalNumOfCellsBD = spark.sparkContext.broadcast(totalNumOfCells)

        /*
        * We Copy every Cell to all possible neigbours internally.
        * We map each fresh new copy to a PartitionID.
        * We keep Only those who map to different Partition. (Up to 4 Copies Maximun for each cell)
        */
        //RDD[partitionID, (CellID, score, isReal)]
        val partitionRDD_1 = cellRDD.mapPartitions {
            tIter => {

                val arrBuff = ArrayBuffer[(Int, (Int, Int, Boolean))]()
                val (minLon, minLat, maxLon, maxLat) = bboxBD.value
                val (minT, maxT) = twBD.value
                val lonBitMask = lonBitMaskBD.value
                val latBitMask = latBitMaskBD.value
                val tBitMask   = tBitMaskBD.value
                val gsLon = gsLonBD.value
                val gsLat = gsLatBD.value
                val gT    = gTBD.value
                val nbSize = nbSizeBD.value
                val lonBits = lonBitsBD.value
                val latBits = latBitsBD.value
                val tBits = timeBitsBD.value

                val pSizeGS = pSizeGSKBD.value
                val pSizeGT = pSizeGTBD.value

                while(tIter.hasNext){

                    val (cellID, score) = tIter.next()

                    try {
                        //What is the PartitionID of the current Cell
                        //CellId -> PID
                        val origPID = cellIDToPID(cellID, pSizeGS, pSizeGT, lonBits, latBits, lonBitMask, latBitMask, tBitMask)

                        //Get the Neighbourhood of the Cell.
                        val cellID_NBSeq = getNBCellIDs(cellID, gsLon, gsLat, gT, nbSize, minLon, minLat, maxLon, maxLat, minT, maxT, lonBits, latBits, lonBitMask, latBitMask, tBitMask)

                        //All Partition Ids of All cells inside Neighbourhood.
                        val pIDUniqSeq = cellID_NBSeq.map(cellID_i => cellIDToPID(cellID_i, pSizeGS, pSizeGT, lonBits, latBits, lonBitMask, latBitMask, tBitMask))
                                                     .distinct

                        var isInPartition = true
                        for(pid_i <- pIDUniqSeq){

                            if (pid_i == origPID)
                                isInPartition = true
                            else
                                isInPartition = false

                            arrBuff.append((pid_i, (cellID, score, isInPartition)))
                        }
                    }
                    catch {
                        case e: Exception => e.printStackTrace()
                    }
                }

                arrBuff.toIterator
            }
        }


        /*
        * We aggregate Per Partition.
        */
        //RDD[partitionRDD, HashMap[CellID, (score, isReal)] ]
        val partitionRDD_2 = partitionRDD_1.aggregateByKey(HashMap[Int, (Int, Boolean)]())(
            //SeqOp
            (hm, x) => {
                hm += ((x._1, (x._2, x._3)))
            },
            //Comb Op
            (hm1, hm2) => {
                hm1 ++= hm2
            }
        )


        //RDD[CellID, Gi*]
        val giStarRDD = partitionRDD_2.flatMap {

            case (pID, hashMap) => {

                var sumWijXj = 0.0
                var sumWij   = 0.0
                var sumWijP2 = 0.0
                val (minLon, minLat, maxLon, maxLat) = bboxBD.value
                val (minT, maxT) = twBD.value


                val lonBitMask = lonBitMaskBD.value
                val latBitMask = latBitMaskBD.value
                val tBitMask   = tBitMaskBD.value
                val gsLon = gsLonBD.value
                val gsLat = gsLatBD.value
                val gT    = gTBD.value
                val nbSize = nbSizeBD.value
                val lonBits = lonBitsBD.value
                val latBits = latBitsBD.value

                val totalNumOfCells = totalNumOfCellsBD.value
                val xMean = xMeanBD.value
                val sMean = sMeanBD.value

                //Replace with Cell-Weight Map
                val nbCellWeight = 1.0

                for {
                    (cellID, (score, isReal)) <- hashMap

                    if (isReal)
                } yield {

                    sumWijXj = 0.0 //For the cell in question wij is exclusively 1! The purpose is to consider with different weight your neighbours from yourself!
                    sumWij   = 0.0
                    sumWijP2 = 0.0

                    //Get the Neighbourhood of the Cell.
                    val cellIDNBSeq = getNBCellIDs(cellID, gsLon, gsLat, gT, nbSize, minLon, minLat, maxLon, maxLat, minT, maxT, lonBits, latBits, lonBitMask, latBitMask, tBitMask)

                    for(cell_i <- cellIDNBSeq){
                        hashMap.get(cell_i) match {
                            case Some(xi) => {
                                sumWijXj = sumWijXj + nbCellWeight * xi._1         //Sum(Wij * Xj) all the cells in the Neighborhood
                                sumWij   = sumWij   + nbCellWeight                 //Sum(Wij) of all the cells in the Neighborhood
                                sumWijP2 = sumWijP2 + nbCellWeight * nbCellWeight  //Sum(Wij^2) of all the cells in the Neighborhood
                            }
                            case None => ()
                        }
                    }

                    val numerator = sumWijXj - xMean * sumWij
                    val denominator = sMean * math.sqrt((totalNumOfCells * sumWijP2 - sumWij * sumWij) / (totalNumOfCells - 1))
                    val gi = numerator / denominator

                    (cellID, gi)
                }
            }
        }



        implicit val sortByMaxGi = new Ordering[(Int, Double)] {

            override def compare(a: (Int, Double), b: (Int, Double)) = {

                if (a._2 > b._2)
                    +1
                else if (a._2 < b._2)
                    -1
                else
                    0
            }
        }

        //Array[(cellID, Score)]
        val top_k_GiArr = giStarRDD.top(top_k)(sortByMaxGi)

        cellRDD.unpersist()
        xMeanBD.destroy()
        sMeanBD.destroy()

        val geometryFactory = new GeometryFactory()

        var i = -1
        val top_k_GeomArrWGS84 = for {
            (cellID, score) <- top_k_GiArr
        } yield {
            val (lon, lat, t) = cellIDToLonLatT(cellID, gsLon, gsLat, gT, minLon, minLat, minTime, lonBits, latBits, timeBits)

            val coordArr = lowerLeftCornerLonLatToGeometry(lon, lat, gsLon, gsLat, geometryFactory)

            i = i + 1

            (i, geometryFactory.createPolygon(coordArr).asInstanceOf[Geometry], t, roundAt(score, 6))
        }

        (top_k_GeomArrWGS84, logArrBuff)


        /*
        * IF We want Compacted Cells --> Array[Geometry, Array(id, t, score)]
        * Only For Zeppelin
        */
        //Temp

        /*
        val compactedArrBuff = ArrayBuffer[(Geometry, ArrayBuffer[(Int, Timestamp, Double)])]()

        for((idx, geometry, t, score) <- top_k_GeomArrWGS84){

            var keepGoing = true
            for((compGeom, arrBuff) <- compactedArrBuff){
                if(keepGoing && geometry.equalsExact(compGeom)) {
                    arrBuff.append((idx, t, score))
                    keepGoing = false
                }
            }

            if(keepGoing){
                compactedArrBuff.append((geometry, ArrayBuffer((idx, t, score)) ) )
            }

        }

        import com.vividsolutions.jts.geom.{Polygon}
        val fArr = compactedArrBuff.map{
            x => {
                x._1 match {
                    case polygon: Polygon => {
                        val coordArr = for (coord <- polygon.getCoordinates) yield{
                            (coord.x, coord.y)
                        }

                        (coordArr, x._2.map(y => (y._1, y._2.toString, y._3)).toArray, x._2.head._1)

                        //case _ => Array[(Double, Double)]()
                    }
                }
            }
        }.toArray
        */

    }

}

