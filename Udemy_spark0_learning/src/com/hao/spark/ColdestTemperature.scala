package com.hao.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

object ColdestTemperature{
  def parse(line:String) :(String,String,Float)={
    val splited=line.split(",")
    val station=splited(0)
    val prop=splited(2)
    val temp=splited(3).toFloat* 0.1f * (9.0f / 5.0f) + 32.0f
    return (station,prop,temp)
  }
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","ColdestTemperature")
    val lines=sc.textFile("../1800.csv")
    val rdd=lines.map(parse);
    val filtered_temp=rdd.filter(x=>x._2=="TMIN");

    
    val dump_redund=filtered_temp.map(x=>(x._1,x._3))//dump TMIN
    val minTempsByStation=dump_redund.reduceByKey((x,y) => min(x,y));
    
    val results = minTempsByStation.collect()
    println("List")
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp") 
    }
  }
}