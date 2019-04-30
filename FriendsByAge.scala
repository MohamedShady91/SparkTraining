package SparkScalaCourse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLine(line:String) ={
    val fields =line.split(",")
    val age=fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala_sundog/fakefriends.csv")
    val rdd=lines.map(parseLine)
    //getting average per year
    val totalsByAge=rdd.mapValues(x=>(x,1)).reduceByKey( (x,y) => (x._1+y._1,x._2+y._2))
    val averagesByAge=totalsByAge.mapValues(x=>(x._1/x._2)).sortByKey()
    val results = averagesByAge.collect()
    results.foreach(println)
    //getting total per year
    val totalfriendsbyage=rdd.reduceByKey((x,y)=>(x+y)).sortByKey()
    val resultss=totalfriendsbyage.collect()
    resultss.foreach(println)






  }
}
