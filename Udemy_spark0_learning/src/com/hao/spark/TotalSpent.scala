package com.hao.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalSpent{
  def parse(line:String) : (Int,Float)={
    val fields=line.split(",");
    return (fields(0).toInt,fields(2).toFloat);
  }
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)//set logger level only printing ERROR
    val sc= new SparkContext("local[*]","Rating_Counter_Learning")
    // create a new SparkContext with multiple cores in the lcoal mode, and the new name is called "Rating...."
    val lines=sc.textFile("../spent.csv")
    val rdd=lines.map(parse);
    val result=rdd.reduceByKey((money1,money2)=>money1+money2).map(x=>(x._2,x._1)).sortByKey();
    result.collect().foreach(println);
  }
}