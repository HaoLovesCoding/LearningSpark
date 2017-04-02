package com.hao.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCount0{
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount");
    val lines=sc.textFile("../book.txt");
    val rdd=lines.flatMap(x=>x.split("\\W+"));
    val result=rdd.map(x=>x.toLowerCase).countByValue();// countByValue is a single row member
    result.foreach(println);
  }
}