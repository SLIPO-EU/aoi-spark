package MySparkContext

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

object mySparkContext {

    val conf = new SparkConf()
    conf.setAppName("Slipo")
    conf.setMaster("local[*]")

    // Enable GeoSpark custom Kryo serializer
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

    //The SparkContext
    val sc = new SparkContext(conf)

}

