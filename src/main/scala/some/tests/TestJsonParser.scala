
package some.tests

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonFactory;
import scala.collection.JavaConversions._
import java.io.File
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.lang.NullPointerException
import java.lang.Exception
import scala.util.{Failure,Success}
import scala.util.Try
import net.liftweb.json.JsonAST
import net.liftweb.json.JsonDSL._
import net.liftweb.json.prettyRender

object TestJsonParser {
  /*  http://code.dblock.org/2017/03/21/whats-the-simplest-way-to-parse-json-in-scala.html
  https://github.com/FasterXML/jackson-module-scala
*/

  /* http://fasterxml.github.io/jackson-core/javadoc/2.9/com/fasterxml/jackson/core/JsonLocation.html
   * https://docs.scala-lang.org/tour/abstract-types.html
   * https://gist.github.com/ConnorDoyle/7002426
   * https://stackoverflow.com/questions/33417061/scala-generics-how-to-declare-that-a-type-must-be-a-case-class
   * https://stackoverflow.com/questions/30705623/how-to-create-object-singleton-of-generic-type-in-scala
   * */

  /* https://www.ngdata.com/parsing-a-large-json-file-efficiently-and-easily/
   * https://www.javadoc.io/doc/com.google.code.gson/gson/2.8.2
   * http://fasterxml.github.io/jackson-core/javadoc/2.9/
   * http://www.acuriousanimal.com/2015/10/23/reading-json-file-in-stream-mode-with-gson.html
   * https://www.tutorialspoint.com/gson/gson_streaming.htm*/

  case class characteristic(name:String,jsonPath:String,fieldtype:String)
  case class CharacteristicsParams(val name: String, val jsonpath: String, val fieldtype: String)
  case class InputArea(val LayoutName:String,val Characteristics:Seq[CharacteristicsParams])
  case class OutputArea(val LayoutName:String,val Characteristics:Seq[CharacteristicsParams])


  
  def getFieldsforValueTypeArray(jsonNode: JsonNode, fieldName: String): Try[List[String]] = {
    val maxItems = jsonNode.path("value").get("maxItems").asInt()
    val numItems = (1 to maxItems)
    val iterator = numItems.toList
    Try(for (i <- iterator) yield fieldName + "[" + i + "]")
  }

  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper()
    val parsedJson = mapper.readTree(new File("D:\\finaltest.json")) //mapper.readTree(new File("D:\\new_json_file_latest.json"))
    val rootNode = parsedJson.path("properties").path("DAJSONDocument").path("properties")
    val layoutNodes = rootNode.fieldNames().toSeq
    val inputNodes = layoutNodes.dropRight(1).filter(x => !x.containsSlice("OCONTROL"))
    val outputNode = layoutNodes.last
    
   try{
    //val y = for(x <- inputNodes) yield InputArea(x,findTablsFieldNode(rootNode,x).flatten)
    //println(y.foreach(print))
    val z = for(x <- inputNodes) yield findTablsFieldNode(rootNode,x)
    z.flatten.flatten.foreach(print)
   }
    catch{
      case e:NoSuchElementException => println("got error "+ e.getMessage,e.getStackTrace.toString())
      case u: Throwable => println("got new error" + u.getMessage)
    }
    //print(y.flatten.foreach(print))
    //val str1 = "{\"SortedDecisionTable\":{\"properties\":{\"data_type\":{\"type\":\"string\",\"required\":true,\"pattern\":\"text\"},\"value\":{\"type\":\"array\",\"maxItems\":20,\"items\":{\"type\":[\"string\",\"null\"]}} },\"additionalProperties\":false}}"
   // val str1 = "{\"properties\":{\"SortedDecisionTable\":{\"properties\":{\"data_type\":{\"type\":\"string\",\"required\":true,\"pattern\":\"text\"},\"value\":{\"type\":\"array\",\"maxItems\":20,\"items\":{\"type\":[\"string\",\"null\"]}} },\"additionalProperties\":false}}}"
     //val parsedJson = mapper.readTree(str1)
     //val str2 = "{\"properties\":{\"someunknownType\":{\"properties\":{\"some_type\":{\"type\":\"string\",\"required\":true,\"pattern\":\"numeric\"},\"some_value\":{\"required\":true,\"type\":[\"number\",\"null\"]}}}}}"
     //val rootjson = mapper.readTree(str2)
     try{
    // getFieldListBasedOnType(rootjson.path("properties"),"someunknownType")
     }
    catch{
      case e:Exception => println("exception in method getFieldListBasedOnType " + e)
    }
    //serializeCollectiontoJson()
  }
  
  def findTablsFieldNode(rootNode:JsonNode,inputAreaName:String):Seq[Seq[CharacteristicsParams]]={
    val inputAreaPropNode = rootNode.path(inputAreaName).path("properties")
    val inputAreaChildNodes = inputAreaPropNode.fieldNames().toSeq
    for(x <- inputAreaChildNodes) yield checkFieldorTableType(inputAreaPropNode,x,inputAreaName)
  }
  
    def checkforChildFields(jsonNode:JsonNode,fields:Seq[String])={
    val currentNode = jsonNode
    val currentChildNames = currentNode.fieldNames().toSeq
    currentChildNames.filter(x => currentNode.path(x).path("properties").fieldNames().toSeq.contains("value"))
  }
  
  def checkFieldorTableType(jsonNode: JsonNode,fieldName:String,inputAreaName:String): Seq[CharacteristicsParams]={
    //println(jsonNode) //input/properties/
    val currentNode = jsonNode.path(fieldName) 
    //println(currentNode) //input/properties//xx//properties//fieldxxx
    val childNodes = currentNode.path("properties").fieldNames().toSeq
    val checkChild = if(childNodes.contains("value")) "field" else "table"
    val prop = if(checkChild.equals("field")) Seq(CharacteristicsParams(fieldName,inputAreaName+"/properties/"+fieldName,"field"))
     else checkLvlofNestingTablType(jsonNode,fieldName ,inputAreaName,inputAreaName+"/properties/"+fieldName)
  prop
  }
  
  
  def checkLvlofNestingTablType(jsonNode:JsonNode,fieldName:String,inputAreaName:String,path:String): Seq[CharacteristicsParams]={
    //val path = inputAreaName+"/properties/"+fieldName+"/properties/"
    val currentNode = jsonNode.path(fieldName).path("properties") 
    if(currentNode.isNull() || currentNode.isMissingNode()){
      throw new NoSuchElementException("reached innermost node, no field/table type found in function checkNestingLevel")
    }
    else{
    //if currentNode.
    //input/properties/account/properties
    val currentNodeChildNames = currentNode.fieldNames().toSeq  //.filter(x => !x.equals("additionalProperties"))
    val nestedfieldty = checkforChildFields(currentNode,currentNodeChildNames)
    val tabletypeChildren = currentNodeChildNames.diff(checkforChildFields(currentNode,currentNodeChildNames))
    if(tabletypeChildren.length > 0){
    val nestedfieldtype = (for(x <- nestedfieldty) yield Seq(CharacteristicsParams(x,path+"/properties/"+x,"field"))).flatten
    
    val nestedtabletype = (for(x <- tabletypeChildren) yield checkLvlofNestingTablType(currentNode,x,inputAreaName,path+"/properties/"+x)).flatten
      
    nestedfieldtype ++ nestedtabletype
    }
    else Seq(CharacteristicsParams(fieldName,path,"table"))
    }
  }
  
  
   def fetchJsonNode(jsonNode: JsonNode, jsonPath: Array[String]): JsonNode = {
     jsonPath.length match{
       case 1 => jsonNode.path(jsonPath(0))
       case n:Int if(n > 1) => fetchJsonNode(jsonNode.path(jsonPath(0)), jsonPath.drop(1))
     }
  }
   
    def getFieldListBasedOnType(fieldParentNode: JsonNode, fieldName: String): Seq[String] = {
    val fieldPropNode = fieldParentNode.path(fieldName).path("properties")
    val fieldValueTypeArr:Try[String] = Try(fieldPropNode.path("value").get("type").toString())
    val fieldValueType:String = if(fieldValueTypeArr.isSuccess) fieldValueTypeArr.get.replace("\"", "").replace("[", "").replace("]", "").split(",")(0) else throw new Exception("value node or type not found")
    val fieldDataType = if (!fieldPropNode.has("data_type") && fieldPropNode.has("value")) "arrayType"
    else if (fieldPropNode.has("data_type")) "simpleType"
    else "unknownType"
     
    def simpleTypeHandler() = {
      fieldValueType.toLowerCase() match {
        case "array"  => List(fieldName)
        case "string" => List(fieldName)
        case "number" => List(fieldName)
      }
    }

    def arrayTypeHandler() = List(fieldName)

    fieldDataType match {
      case "simpleType"  => simpleTypeHandler()
      case "arrayType"   => arrayTypeHandler()
      case "unknownType" => throw new Exception("unknown field data type detected")
    }
  }
     
  def serializeCollectiontoJson()={
        val map = List(Map("fname" -> List("Alvin","Alexander"), "lname" -> List("Alexander")))
        println(map)
        println(JsonAST.compactRender(map))
        println(JsonAST.render(map))

  }
  
    def nestedFieldOrTableCheck(jsonNode:JsonNode,fieldName:String,inputAreaName:String,path:String): Seq[CharacteristicsParams]={
    val currentNode = jsonNode.path(fieldName) 
    val childNodes = currentNode.path("properties").fieldNames().toSeq
    val checkChild = if(childNodes.contains("value")) "field" else "table"
    val prop = if(checkChild.equals("field")) Seq(CharacteristicsParams(fieldName,path+"/properties/"+fieldName,"field")) 
     else checkLvlofNestingTablType(currentNode,fieldName ,inputAreaName,path+"/properties/"+fieldName)
     prop
  }
      
}

  
