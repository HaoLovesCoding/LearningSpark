package com.hao.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCount1{
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount");
    val lines=sc.textFile("../book.txt");
    val rdd=lines.flatMap(x=>x.split("\\W+"));
    //val result=rdd.map(x=>x.toLowerCase).countByValue();// countByValue is a single row member
    val lowerCount=rdd.map(x=>x.toLowerCase).map(x=>(x,1)).reduceByKey((count1,count2)=>count1+count2);
    val CountToWord=lowerCount.map(x=>(x._2,x._1)).sortByKey();
    
    CountToWord.collect();// collect must be before foreach
    
    CountToWord.foreach(println);
    println("another way to print out")
    for (result <- CountToWord) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}