package com.hao.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object RatingCounter_Learning{
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)//set logger level only printing ERROR
    
    val sc= new SparkContext("local[*]","Rating_Counter_Learning")
    // create a new SparkContext with multiple cores in the lcoal mode, and the new name is called "Rating...."
    val lines=sc.textFile("../ml-100k/u.data")
    //load file into lines
    val ratings=lines.map(x=>x.toString().split("\t")(2))
    //map the lines into a column of ratings
    val results=ratings.countByValue()
    //count the unique value
    println("Rating, Number")
    
    for(result <- results){
      println(result);
    }
    
    println("Now sort the results by rating")
    
    val sortedResults=results.toSeq.sortBy(_._1)// convert RDD to sequence data structure
    
    for(result <- sortedResults){
      println(result);
    }
    
  }
}

