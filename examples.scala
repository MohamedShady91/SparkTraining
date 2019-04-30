package SparkScalaCourse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**simple examples to start developing based on spark*/

object examples {

  def squares5(x:Int):Int={
    x*x
  }
  def main(args:Array[String])
  {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "example1")
    val rdd= sc.parallelize(List(1,2,3,4))
    val squares = rdd.map(x=>x*x)
    val squares2=rdd.map(squares5)
    squares.foreach(println)
    squares2.foreach(println)

println("------------------------------------------")
    val rdd2=sc.parallelize(List(20,25,25,24,23,27,60,35))
    val totalbyage = rdd2.map(x=>(x,1))
    val tot = totalbyage.reduceByKey((x,y)=>x+y)
    val tot2= tot.sortByKey()
    val tot3= totalbyage.groupByKey()
    tot2.foreach(println)
    tot3.foreach(println)
  }
}
