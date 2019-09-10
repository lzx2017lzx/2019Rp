package com.lzx.load



case class Movie(val mid:Int, val name:String, val descri:String,
                 val timelong:String, val issue:String, val shoot:String,
                 val language:String, val genres:String, val actors:String,
                 val directors:String)

case class Rating(val uid:Int, val mid:Int, val scores:Double, val timestamp:Int)

case class Tag(val uid:Int, val mid:Int, val tag:String, val timestamp: Int)

case class MongoConfig(val uri:String, val db:String)

case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)