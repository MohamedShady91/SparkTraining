package SparkScalaCourse

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AmountSpentByCustomer {

  def parser(line:String)={
    val parsedLine = line.split(",")
    val custID = parsedLine(0).toInt
    val amount = parsedLine(2).toFloat
    (custID,amount)
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("../SparkScala_sundog/customer-orders.csv")

    //convert lines to tuples of customerID,amount
    val parsedLines = lines.map(parser)

    val totalAmountSpent = parsedLines.reduceByKey((x,y)=>x+y)

    val result=totalAmountSpent.map(x=>(x._2,x._1))
    val finalResult=result.sortByKey().collect()
    finalResult.foreach(println)


  }

  }
