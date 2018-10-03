package com.scala.practise

//https://www.javadoc.io/doc/com.google.code.gson/gson/2.8.2
//http://goessner.net/articles/JsonPath/
//https://static.javadoc.io/com.google.code.gson/gson/2.8.2/com/google/gson/stream/JsonToken.html
//https://static.javadoc.io/com.google.code.gson/gson/2.8.2/com/google/gson/stream/JsonToken.html#NAME
//https://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/

import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import com.google.gson.stream._
import java.io.BufferedReader
import java.io._

object JSONTokenApproach {

/*  def parseJson() = {
    val br = new BufferedReader(new FileReader("D:\\newjson.json"))
    val in = new InputStreamReader(new FileInputStream("D:\\newjson.json"))
    val parser = new JsonParser()
    val parsed = parser.parse(in)
    println(parsed.toString())
    if (parsed.isJsonObject()) {
      println("yes json object")
      val child = parsed.getAsJsonObject
      val prop = parsed.getAsJsonObject.get("properties")
      if (prop.isJsonObject()) {
      }
      println(prop.toString())
      if (!child.isJsonObject()) {
        println(child.toString())
      }
    }
  }*/
  
  def simpleParser():Unit={
     val reader = new JsonReader(new InputStreamReader(new FileInputStream("D:\\simplejson.json"), "UTF-8"));
     fieldParser(reader)
  }
  
  
  def fieldParser(reader:JsonReader):Unit ={
    reader.beginObject()
    val token = reader.nextName()
    if(token.matches("data_type")){
      println("found field, skip")
      reader.skipValue()
      while(reader.hasNext()){
        val token1 = reader.peek()
        if(token1.equals(JsonToken.END_OBJECT))
        {
          println("end object")
          reader.endObject()
        }
        else{
      reader.skipValue()
      println("skipping other names inside field property")
        }
      }
       //reader.endObject()
    }
  }
  

  def jsonObjectParser(reader: JsonReader):Unit = {
    reader.beginObject()
    while (reader.hasNext()) {
      val ocontrol = reader.nextName()
      println("ocontrol" + ocontrol)
      println(reader.getPath)
      if(ocontrol.equals("OCONTROL")){
      reader.skipValue()
    }
      else{ 
          if(!(ocontrol.equals("data_type")||ocontrol.equals("value")|| ocontrol.equals("additionalProperties")|| ocontrol.equals("date_format"))) {
          jsonObjectParser(reader)
          }
          else{
            println(reader.getPath)
            reader.skipValue()
          }
          
      }
  }
    reader.endObject()
  }

  def jsonReader() = {
    //new InputStreamReader(new StringReader("string"))
    val reader = new JsonReader(new InputStreamReader(new FileInputStream("D:\\newjson.json"), "UTF-8"));

    reader.beginObject()
    while (reader.hasNext()) {
      val propname = reader.nextName()
      println(propname)
      if (propname != "properties") {
        reader.skipValue()
      } else {
        reader.beginObject()
        while (reader.hasNext()) {
          val controlname = reader.nextName()
          println(controlname)
          if (controlname != "DAJSONDocument") {
            reader.skipValue()
          } else {
            reader.beginObject()
            while (reader.hasNext()) {
              val prop1name = reader.nextName()
              println(prop1name)
              println(reader.getPath)
              if (prop1name != "properties") {
                reader.skipValue()
              } else {
                jsonObjectParser(reader)
                }
              }
            }
          }
        reader.endObject()
        }
      }
  
    reader.endObject()
    reader.close()
  }

  def main(args: Array[String]): Unit = {
    //parseJson()
    //jsonReader()
    simpleParser()
  }

}