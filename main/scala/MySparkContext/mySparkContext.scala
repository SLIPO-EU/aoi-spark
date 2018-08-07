package MySparkContext

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer


object mySparkContext {

    val conf = new SparkConf()

    conf.setAppName("Runnable") // Change this to a proper name
    conf.setMaster("local[*]") // Delete this if run in cluster mode

    // Enable Kryo serializer
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    //conf.set("spark.driver.extraJavaOptions", "-XX:+UseCompressedOops")
    //conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

    //The SparkContext
    val sc = new SparkContext(conf)

}

