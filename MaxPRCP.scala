package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the minimum temperature by weather station */
object MaxPRCP {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val pcrp = fields(3)
    (stationID, entryType, pcrp)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../SparkScala_sundog/1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but PRCP entries
    val maxPrcp = parsedLines.filter(x => x._2 == "PRCP")
    
    // Convert to (stationID, prcp)
    val stationTemps = maxPrcp.map(x => (x._1, x._3.toInt))
    
    // Reduce by stationID retaining the max PRCP found
    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))
    
    // Collect, format, and print the results
    val results = maxTempsByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val prcp = result._2
       println(s"$station maximum PRCP: $prcp")
    }
      
  }
}