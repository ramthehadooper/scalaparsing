package com.datacrop.sc
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object GroupBy {
  
  
  def main(args: Array[String]): Unit = {

    /*
     * This program parses a string and picks up the min(VSL12345) ie first begining startpoint and max(YRD35900)ie final ending point. It ignores the other 
     * points the container has travelled. The begining/Ending event is ascertained by the min and max of the id 240220171, 240220172, 240220900
     */
    val v1 = "240220171-VSL12345-YRD35671,240220172-YRD35671-YRD35672,240220173-YRD35671-YRD35673,240220900-YRD35671-YRD35900"
    val v2 = List(v1.split(","))
    
    for (e <- v2){
    
     var max:String = ""
     var min:String = ""
     var y1:Map[String, String] = Map()


       e foreach { x =>  
         val y= x.split(",")
         y1 = y1 + (y(0) ->  x) //Addition of elements to the Map
         
       }
       
       max = y1(y1.keysIterator.max)
       min = y1(y1.keysIterator.min)

       val finalVal =  List(
                             List(y1(y1.keysIterator.min)).map { x => x.split("-") }.map { x => x(0) }.mkString(" ") + "," +
                             List(y1(y1.keysIterator.min)).map { x => x.split("-") }.map { x => x(1) }.mkString(" ") + "," +
                             List(y1(y1.keysIterator.max)).map { x => x.split("-") }.map { x => x(2) }.mkString(" ")
                       ).mkString(" ")
       
       println(finalVal)
       println("OuterLoop")
    }
    
  }
}