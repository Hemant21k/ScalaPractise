/*package com.scala.practise
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

import scala.collection.mutable.LinkedHashMap


object JSONObjectParser {
  
  var op = LinkedHashMap[String, String]()
  
  def parse(json: JSONObject,out:LinkedHashMap[String,String]): LinkedHashMap[String,String]= {
   //var out = Map[String, String]()
   val keys  = json.keys();
    while(keys.hasNext()){
        var key = keys.next();
        var value = "";
if ( json.get(key).isInstanceOf[JSONObject] ) {
    val value:JSONObject  = json.getJSONObject(key);
    parse(value,op);
} 
else {
    
     value = json.get(key).toString()
}
        if(value != null){
            op += (key->value);
        }
    }
    
    //println(out)
    return op;
}
  
def main(args:Array[String]){
  val f = new File("D:\\jsonfile.json");
        if (f.exists()){
           val is = new FileInputStream("D:\\jsonfile.json");
            val jsonTxt = IOUtils.toString(is, "UTF-8");
            val partsData = new JSONObject(jsonTxt)
            println(partsData)
            val map = LinkedHashMap[String,String]()
            println(parse(partsData,map))
        }

}
  
}*/