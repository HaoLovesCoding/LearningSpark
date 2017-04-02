package com.hao.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByAge{
  
  // parse function will return a tuple of age and friend
  def parse(line:String) : (Int,Int)={
    val splited : Array[String]=line.split(",")// will split line into several chunks
    val age=splited(2).toInt
    val number_friend=splited(3).toInt
    return (age,number_friend)
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","FriendsByAge")
    val lines=sc.textFile("../fakefriends.csv")
    val rdd=lines.map(parse)
    // rdd now includes a lot of key value pair of age and number
    val totalFriendByAge=rdd.mapValues(friend => (friend,1)).reduceByKey((tup1,tup2)=>(tup1._1+tup2._1,tup1._2+tup2._2))
    // rdd has a lot of key value pairs (age,friend)
    // mapValues means map the friend to (friend), then it becomes (age,(friend,1))
    // reduceByKey will group every same key and perform the closure in the ()
    // here same age is group together and tup1 tup2 are all (friend,1)
    // then friend is summed 1 is also summed
    val averageFriendByAge=totalFriendByAge.mapValues( tup=> tup._1/tup._2 )// the tuple is 1-based
    val results = averageFriendByAge.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }
}