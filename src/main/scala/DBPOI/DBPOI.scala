package DBPOI

import Enumerators.dbstatusEnum._

case class DBPOI(val poiId: String,
                 val lon: Double,
                 val lat: Double){

    var dbstatus    = UNDEFINED
    var isDense     = false
    var isBoundary  = false
    var clusterName = ""

    override def toString: String = {
        s"poiID: $poiId, lon: $lon, lat: $lat, dbStatus: $dbstatus, isDense: $isDense, isBoundary: $isBoundary, clusterName: $clusterName"
    }
}
