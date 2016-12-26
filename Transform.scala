import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.util.control.Breaks._
import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.collections.bag.SynchronizedSortedBag

object Transform {
  def main(args: Array[String]) {

    //val config = ConfigFactory.parseFile(new File("Config.conf"))

    val conf = new SparkConf()
    conf.set("es.index.auto.create","true")
      //conf.set("spark.serializer", classOf[KryoSerializer].getName)
      .setAppName("Test Data Analysis")
      .setMaster("local[4]")

    conf.set("es.nodes", "10.71.71.176")
    val sc = new SparkContext(conf)

    val esRDDRatings = sc.esRDD("movie_prediction_final/ratings", "")
    val esRDDMovies = sc.esRDD("movie_prediction_final/movies", "")
    //val mapped = esRDDRatings.map(x => (x._2("movieId"), (x._2("rating").toDouble, 1)))//.reduceByKey((x, y) => (x._2 + y._2)).take(10).foreach(println)
    val countRows = esRDDRatings.map(x => (x._2("movieId"),1))

    //number of rating per movie
    val countMovies = countRows.map(n => (n._1, n._2.toInt)).reduceByKey((v1,v2) => v1 + v2)
    //countMovies.map(x => (x._1, x._2)).join(esRDDMovies.map(x => (x._2("movieId"), x._2("title")))).map(x => (x._2._2, x._2._1)).foreach(println)

    //average rating per movie
    val sumMovies = esRDDRatings.map(x => (x._2("movieId"), x._2("rating").asInstanceOf[Double])).reduceByKey((v1,v2) => v1 + v2)
    val joined = sumMovies.map(x => (x._1, x._2)).join(countMovies.map(x => (x._1, x._2)))
    val average = joined.map { x =>; val temp = x._2; val total = temp._1; val count = temp._2; (x._1, math.ceil(total / count))} //average rating per movie
    val finalRatings = sc.makeRDD(average.map(x => (x._2, x._1)).sortByKey(false).take(100)) //number of rating per movie sorted according to ratings
    //finalRatings.map(x => (x._2, x._1)).join(esRDDMovies.map(x =>(x._2("movieId"), x._2("title")))).map(x => (x._2._1, (x._1, x._2._2))).sortByKey(false).map(x => (x._2._1, x._2._2, x._1)).foreach(println)
    //finalRatings.foreach(println)
    //Movies with highest average rating
    //finalRatings.map(x => (x._2, x._1)).join(esRDDMovies.map(x =>(x._2("movieId"), x._2("title")))).map(x => (x._2._1, (x._1, x._2._2))).sortByKey(false).map(x => (x._2._1, x._2._2, x._1)).take(10).foreach(println)

    //Highest avergae ratings with more than 500 reviews
    //val joinCountAvg = countMovies.map(x => (x._1, x._2)).join(average.map(x => (x._1, x._2))).map(x => (x._2._2 ,(x._1, x._2._1))).sortByKey(false).filter(x => (x._2._2 > 2)).map(x => (x._2._1, x._1)).join(esRDDMovies.map(x => (x._2("movieId"), x._2("title")))).map(x => (x._2._1, x._2._2)).sortByKey(false).take(10).foreach(println)

    val splitGenres = esRDDMovies.map(x => ((x._2("movieId"), x._2("title")), x._2("genres").toString.split("\\|"))).flatMapValues(x => x)
    val flatMovieRDD = splitGenres.map(x => (x._1._1, x._1._2 , x._2)) //flatten movie RDD by genres
    val RateMovieRDD = finalRatings.map(x => (x._2, x._1)).join(flatMovieRDD.map(x => (x._1,(x._2, x._3))))
    val flatRateMovieRDD = RateMovieRDD.map(x => (x._1 ,x._2._1, x._2._2._1, x._2._2._2))

    //Per genres highest rated movies
    val perGenreRate = flatRateMovieRDD.map(x => (x._4, (x._2))).reduceByKey((x, y) => (math.max(x, y)))
    //perGenreRate.foreach(println)
    //perGenreRate.map(x => ((x._1, x._2), None)).join(flatRateMovieRDD.map(x => ((x._4, x._2), (x._1, x._3)))).map(x => (x._1._1, x._2._2._2, x._2._2._1)).take(10).foreach(println)

    //val mapped1 = mapped.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).take(10).foreach(println)
    //mapped1.map(x => (x._1, x._2)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).take(10).foreach(println)
    // val reduced = mapped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).take(10).foreach(println)
    //val rdd1 = esRDDRatings.map(x => (x._2("movieId"), (x._2("userId"), x._2("rating"), x._2("timestamp")))).take(10).foreach(println)
    //val rdd2 = esRDDMovies.map(x => (x._2("movieId"), (x._2("title"), x._2("genres")))).take(10).foreach(println)


   // var i = 0
    //do
    //{

    println("Printing Count:")

    println("1. Number of rating per movie")
    println("2. Average rating per movie")
    println("3. Movies with highest average rating")
    println("4. Highest average ratings with more than 500 reviews")
    println("5. Per genres highest rated movies")

    //println("Enter your choice")
    //i = Console.readInt

        println("Hello in switch ...")
        val countRDD = sc.makeRDD(countMovies.map(x => (x._1, x._2)).join(esRDDMovies.map(x => (x._2("movieId"), (x._2("title"), x._2("genres"))))).map(x => (x._2._2._1, x._2._2._2, x._2._1)).map(x => (Map("title" -> x._1, "genres" -> x._2, "count" -> x._3))).take(10))
        println("Built CountRDD Object ")
        //countRDD.foreach(println)
        countRDD.saveToEs("per_movie_count/PerMovieCount")
        println("Save to ES Called...")
        //countRDD.take(countRDD.count.toInt).foreach(println)
        println("Printing each record.....")
        println("Hello")
        val averageRDD = sc.makeRDD(finalRatings.map(x => (x._2, x._1)).join(esRDDMovies.map(x =>(x._2("movieId"), (x._2("title"), x._2("genres"))))).map(x => (x._2._1, (x._2._2._1, x._2._2._2))).sortByKey(false).map(x => (x._2._1, x._2._2, x._1)).map(x => (Map("title" -> x._1, "genres" -> x._2, "average" -> x._3))).take(10))
        averageRDD.saveToEs("per_movie_avg/PerMovieAvg")
        averageRDD.take(averageRDD.count.toInt).foreach(println)
        val highRDD = sc.makeRDD(finalRatings.map(x => (x._2, x._1)).join(esRDDMovies.map(x =>(x._2("movieId"), (x._2("title"), x._2("genres"))))).map(x => (x._2._1, (x._2._2._1, x._2._2._2))).sortByKey(false).map(x => (x._2._1, x._2._2, x._1)).map(x => (Map("title" -> x._1, "genres" -> x._2, "average" -> x._3))).take(10))
        //highRDD.foreach(println)
        highRDD.saveToEs("per_movie_avg_highest/PerMovieAvgHighest")
        val reviewRDD = sc.makeRDD(countMovies.map(x => (x._1, x._2)).join(average.map(x => (x._1, x._2))).map(x => (x._2._2 ,(x._1, x._2._1))).sortByKey(false).filter(x => (x._2._2 > 2)).map(x => (x._2._1, x._1)).join(esRDDMovies.map(x => (x._2("movieId"), (x._2("title"), x._2("genres"))))).map(x => (x._2._1, (x._2._2._1, x._2._2._2))).sortByKey(false).map(x => (Map("average" -> x._1, "title" -> x._2._1, "genres" -> x._2._2))).take(10))
        //reviewRDD.foreach(println)
        reviewRDD.saveToEs("highest_rating_review/HighestRatingReview")
        val GenreRDD = sc.makeRDD(perGenreRate.map(x => ((x._1, x._2), None)).join(flatRateMovieRDD.map(x => ((x._4, x._2), (x._1, x._3)))).map(x => (x._1._1, x._2._2._2, x._2._2._1)).map(x => (Map("genres" -> x._1, "title" -> x._2, "movieId" -> x._3))).take(10))
        GenreRDD.foreach(println)
        GenreRDD.saveToEs("per_genres_highest_movies/PerGenresHighestMovies")


    //}while(i > 6)
    System.out.println("Executed ..2");
    sc.stop()

  }
}
