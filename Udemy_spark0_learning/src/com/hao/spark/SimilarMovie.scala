package com.hao.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object SimilarMovie{
  def getMovieMap() : Map[Int,String]={
    var result:Map[Int,String]=Map();
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE) // This is to handle the input of line<-lines
    
    val lines=Source.fromFile("../ml-1m/movies.dat").getLines();
    for (line<-lines){
      val fields=line.split("::");
      if(fields.length>1){
        result +=((fields(0).toInt->fields(1)));
      }
    }
    return result;
  }
  
  def myfilter(node : (Int, ((Int,Double),(Int,Double)) ) ) : Boolean= {
    val movieID_rating_pair1=node._2._1;
    val movieID_rating_pair2=node._2._2;
    return movieID_rating_pair1._1<movieID_rating_pair2._1;
  }
  
  def switchpair(node: (Int, ((Int,Double),(Int,Double)) )) : ((Int,Int),(Double,Double)) ={
    val pair1=node._2._1;
    val pair2=node._2._2;
    val movie1=pair1._1;
    val movie2=pair2._1;
    val rating1=pair1._2;
    val rating2=pair2._2;
    return ((movie1,movie2),(rating1,rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def getMovieSim(rating_pairs:RatingPairs):(Double,Int) = {
    var pair_count:Int=0;
    var xx_sum:Double=0;
    var yy_sum:Double=0;
    var cross_product_sum:Double=0;
    for(pair<-rating_pairs){
      val ratingX=pair._1;
      val ratingY=pair._2;
      xx_sum+=ratingX*ratingX;
      yy_sum+=ratingY*ratingY;
      cross_product_sum+=ratingX*ratingY;
      pair_count+=1;
    }
    val normalization_denominator=sqrt(xx_sum)*sqrt(yy_sum);
    var score:Double=0;
    if(normalization_denominator!=0){
      score=cross_product_sum/normalization_denominator;
    }
    return (score,pair_count);
  }
  
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR);
    val conf=new SparkConf();
    conf.setAppName("SimilarMovie");
    val sc=new SparkContext("local[*]","SimilarMovie");
    
    val MovieMap=getMovieMap();
    
    val data = sc.textFile("../ml-1m/ratings.dat");// load the rating data
    
    val rating_init_rdd=data.map(x=>x.split("::")).map(x=>(x(0).toInt,(x(1).toInt,x(2).toDouble)) );// (ID,(movieId,rating));
    
    val joined_rdd=rating_init_rdd.join(rating_init_rdd);// Spark join is SQL join performed on the same key
    
    val filtered_rdd=joined_rdd.filter(myfilter); // This will filter half of the symetric entry
    // (ID, (movieID1,rating1),(movieID2,rating2))
    
    //Now we switch pair and remove ID (user ID), format is : (movieID1,movieID2),(rating1,rating2)
    val switched_rdd=filtered_rdd.map(switchpair).partitionBy(new HashPartitioner(100))
    
    //groupByKey is slightly different from SQL, it will aggregate an 
    val movieSim=switched_rdd.groupByKey().mapValues(getMovieSim).cache();
    
    //now format is (movieID1,movieID2), (score, co_appearance_count)
    
    if(args.length<1){
      println("Usage: Error!");
      return;
    }
    
    val score_threshold=0.97
    val coappearance_threshold=1000;
    
    val cur_movieID=args(0).toInt;
    
    val chosen_movies=movieSim.filter(
      x=>{
        val score=x._2._1
        val co_appearance=x._2._2
        ( ((x._1._1==cur_movieID)||(x._1._2==cur_movieID)) && (score>score_threshold) && (co_appearance>coappearance_threshold) )
      }
    )
    // now we have the list for recommendation
    
    //After map it is (score, appreance_time), (movieID1, movieID2), 
    //then sortByKey (key is tuple, so first sort by the first item, then secon)
    val sorted_rdd=chosen_movies.map(x=>(x._2,x._1)).sortByKey(false).take(50);
    
    println("The top recommended movies for "+ MovieMap(cur_movieID));
    for(pair<-sorted_rdd){
      val movieID1=pair._2._1;
      val movieID2=pair._2._2;
      //println("movie");
      //println(movieID1);
      //println(movieID2);
      var result_movieID=movieID1;
      if(result_movieID==cur_movieID){
        result_movieID=movieID2;
      }
      println(MovieMap(result_movieID) + "similarity: " +pair._1._1 +"co_appreance: "+pair._1._2);
    }
  }
}