package com.hao.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

object BFS{
  val startID=5306; //SpiderMan
  val endID=14;
  var foundCounter:Option[LongAccumulator] = None;// The foundCounter will be set not None once the endID is found
  type BFSData = (Array[Int], Int, String);// BFSData will have the (neighborID, distance, color)
  type BFSNode = (Int,BFSData);// The (Self ID, Data)
  
  def convertBFSNode(line:String) : BFSNode={
    val fields=line.split("\\s+");
    val self_ID=fields(0).toInt;
    var neighborlist: ArrayBuffer[Int] = ArrayBuffer()
    for(j <- 1 to (fields.length-1) ){
      neighborlist+=fields(j).toInt;
    }
    if(self_ID==startID){
      return (self_ID,(neighborlist.toArray,0,"GRAY"));// If this is the node to start, we have label it with gray
    }
    else{
      return (self_ID,(neighborlist.toArray,9999,"WHITE"));
    }
  }
  
  def createStartRDD(sc:SparkContext) : RDD[BFSNode]={
    val lines=sc.textFile("../Marvel-graph.txt");
    val BFS_RDD=lines.map(convertBFSNode);
    return BFS_RDD;
  }
  
  def BFSmap(cur_node:BFSNode):Array[BFSNode]={ // This function will be used in the flat map, so one Node to an Array
    var result : ArrayBuffer[BFSNode]=ArrayBuffer();
    
    val cur_ID=cur_node._1;
    val cur_data=cur_node._2;
    
    val neighbors_ID=cur_data._1;
    val cur_distance=cur_data._2;
    var cur_color=cur_data._3;
    
    if(cur_color=="GRAY"){// all the node to be visited is marked as GREY
      
      for(neighbor_ID<-neighbors_ID){
        val friend_ID=neighbor_ID;
        val friend_neighbor : Array[Int]=Array();// Must declare the type!! 
        val friend_distance : Int =cur_distance+1;
        val friend_color="GRAY";
        
      // signal to all cluster that the target is found
        if(neighbor_ID==endID){
          //println("found1")
          if(foundCounter.isDefined){
            //println("found2")
            foundCounter.get.add(1);//+=1 can not apply to long
          }         
        }
        val newNode:BFSNode= (friend_ID,(friend_neighbor,friend_distance,friend_color));
        result+=newNode;
        
      }
      cur_color="BLACK"; // means this node has been found
    }
    val self_visited_Node : BFSNode = (cur_ID,(neighbors_ID,cur_distance,cur_color));// CANNNOT add directly
    result += self_visited_Node;
    return result.toArray;
  }
  
  def BFSreduce(data1:BFSData, data2: BFSData) : BFSData={
    var neighbor_collection: ArrayBuffer[Int]= ArrayBuffer();
    var distance:Int=9999;
    var color:String="";
    
    val neighbor1=data1._1;
    val neighbor2=data2._1;
    val distance1=data1._2;
    val distance2=data2._2;
    val color1=data1._3;
    val color2=data2._3;
    
    if(neighbor1.length>1){
      neighbor_collection++=neighbor1;
    }
    if(neighbor2.length>1){
      neighbor_collection++=neighbor2;
    }
    
    if(distance1<distance){
      distance=distance1;
    }
    if(distance2<distance){
      distance=distance2;
    }
    
    if(color1=="WHITE"){
      color=color2;
    }
    if(color1=="GRAY"){
      if(color2=="BLACK"){
        color=color2;
      }
      else{
        color=color1;
      }
    }
    if(color1=="BLACK"){
      color=color1;
    }
    
    var result : BFSData= (neighbor_collection.toArray,distance,color);
    return result;
  }
  
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc=new SparkContext("local[*]","BFS");
    var RDD1=createStartRDD(sc);// The RDD is (ID, BFSData) type
    var iteration:Int=0;
    // The use map and reduce to finish one BFS operation
    foundCounter = Some(sc.longAccumulator("Hit Counter"))// It must be initilized and also an optional parameter in class
    
    while(iteration<10){
      println("Running BFS Iteration# " + iteration)
      
      iteration+=1;
      var visited_RDD :RDD[BFSNode] =RDD1.flatMap(BFSmap);
      //visited_RDD.collect();// must execute to have foundCounter updated
      
      println("Processing " + visited_RDD.count() + " values.")
      
      if(foundCounter.isDefined){
        var count=foundCounter.get.value;
        if(count>0){
        println("Hit the target character! From " + count + 
              " different direction(s).")
          return;
        }
      }
      
      RDD1=visited_RDD.reduceByKey(BFSreduce);
      
    }
    
  }
}