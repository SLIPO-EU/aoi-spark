package DBSCAN

/*
* DBSCAN Distributed Edition in Spark & Scala.
*
* Authors: Panagiotis Kalampokis, Dr. Dimitris Skoutas
* */

import DBPOI.DBPOI
import Grid.Grid
import scala.collection.mutable.ArrayBuffer
import Enumerators.dbstatusEnum._
import scala.collection.mutable


case class DBCLusterer(val eps: Double, val minPts: Int) {

    def clusterPois(poiArrBuff: ArrayBuffer[DBPOI]) = {

        val clusterArrBuff = ArrayBuffer[ArrayBuffer[DBPOI]]()
        val grid = Grid(poiArrBuff, eps)

        for{
            dbpoi <- poiArrBuff

            if(dbpoi.dbstatus == UNDEFINED)
        }{

            val neighbourArrBuff = grid.getNeighbours(dbpoi)

            if(neighbourArrBuff.size < minPts)
                dbpoi.dbstatus = NOISE
            else
                clusterArrBuff.append(findCluster(dbpoi, neighbourArrBuff, grid))
        }

        clusterArrBuff
    }


    def findCluster(dbpoi: DBPOI, neighbourArrBuff: ArrayBuffer[DBPOI], grid: Grid): ArrayBuffer[DBPOI] = {

        dbpoi.dbstatus = PARTOFCLUSTER
        dbpoi.isDense  = true

        val cluster = ArrayBuffer[DBPOI]()
        cluster.append(dbpoi)

        val neighbourQueue = mutable.Queue[DBPOI]() ++ neighbourArrBuff

        while(neighbourQueue.nonEmpty){
            val poi = neighbourQueue.dequeue()

            poi.dbstatus match {

                case UNDEFINED => {
                    poi.dbstatus = PARTOFCLUSTER
                    val poi_i_neighbours = grid.getNeighbours(poi)

                    if(poi_i_neighbours.size >= minPts) {
                        poi.isDense  = true
                        neighbourQueue ++= poi_i_neighbours
                    }
                    else
                        poi.isDense  = false

                    cluster.append(poi)
                }

                case NOISE => {
                    poi.dbstatus = PARTOFCLUSTER
                    poi.isDense  = false

                    cluster.append(poi)
                }
                case _ => ()
            }
        }

        cluster
    }
    
}

