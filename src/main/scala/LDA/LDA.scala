package LDA

import org.apache.spark.rdd.RDD
import mySparkSession.mySparkSession
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import scala.collection.mutable


case class LDA() extends Serializable {

    //           RDD[  id,      Array[kwds] ]
    def lda(rdd: RDD[(String, Array[String])], num_of_topics: Int) = {

        import mySparkSession.spark_session.implicits._

        val df = rdd.toDF("id", "words")

        // fit a CountVectorizerModel from the corpus
        val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words")
                                                                 .setOutputCol("features")
                                                                 .fit(df)

        val cvDF = cvModel.transform(df)
        //println(s"Vocabulary: [${cvModel.vocabulary.mkString(",")}]")

        // Trains a LDA model.
        val lda = new org.apache.spark.ml.clustering.LDA().setK(num_of_topics).setMaxIter(10).setOptimizer("em")

        val lda_model = lda.fit(cvDF)
        val topicsDF = lda_model.describeTopics()

        val topic_arr = topicsDF.collect()
        val voc = cvModel.vocabulary.zipWithIndex.map(_.swap).toMap

        val topic_arr_2 = topic_arr.map{
            row => {
                val terms = row(1).asInstanceOf[mutable.WrappedArray[Int]].map(t_i => voc(t_i))
                (row(0).asInstanceOf[Int], terms, row(2).asInstanceOf[mutable.WrappedArray[Double]])
            }
        }

        val topicRDD = mySparkSession.sparkContext.parallelize(topic_arr_2)
                                                  .map(t => Row(t._1, t._2, t._3) )

        val schema = StructType(Seq(
            StructField("topic", IntegerType, false),
            StructField("terms", ArrayType(StringType, true), false),
            StructField("termWeights", ArrayType(DoubleType, false))
        ))

        val topicDF_2 = mySparkSession.spark_session.createDataFrame(topicRDD, schema)


        var transformed_DF = lda_model.transform(cvDF).drop($"features")

        transformed_DF = transformed_DF.map{
            row => {
                val topicValues = row(2).asInstanceOf[org.apache.spark.ml.linalg.DenseVector].values
                val domTopic = topicValues.zipWithIndex.foldLeft((-1.0, -1))((z, t) => if(z._1 > t._1) z else t)

                (row(0).asInstanceOf[String], row(1).asInstanceOf[scala.collection.mutable.WrappedArray[String]].toArray, topicValues, domTopic._2)
            }
        }.toDF("id", "words", "topicDistribution", "dominantTopic")


        val elements = (0 until(num_of_topics)).map(i => s"topic_$i").toArray

        // Create a SQL-like expression using the array
        val sqlExpr = Array(col("id"), col("words")) ++ elements.zipWithIndex.map{ case (alias, idx) => col("topicDistribution").getItem(idx).as(alias) } :+ col("dominantTopic")
        val finalDF = transformed_DF.select(sqlExpr : _*)

        (topicDF_2, finalDF)
    }

}


