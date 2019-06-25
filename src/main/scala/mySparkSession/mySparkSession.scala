package mySparkSession

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

object mySparkSession {

    val spark_session = SparkSession.builder()
                                    .appName("Slipo")
                                    //Enable GeoSpark custom Kryo serializer
                                    .config("spark.serializer", classOf[KryoSerializer].getName)
                                    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
                                    .config("spark.driver.memory", "4g")
                                    .config("spark.executor.memory", "4g")
                                    .master("local[*]")
                                    .getOrCreate()

    val sparkContext = spark_session.sparkContext
}

