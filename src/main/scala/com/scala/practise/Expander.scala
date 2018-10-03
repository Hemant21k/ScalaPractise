package com.scala.practise

import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.Mirror
import scala.reflect.classTag
import scala.language.existentials
//import scala.reflect.runtime.currentMirror



object Expander {
  
  
  def createCaseClass(name:String) = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val tb = ToolBox(mirror).mkToolBox()
    val src = "case class person(name:String,age:Int){}; scala.reflect.classTag[person].runtimeClass;"
    val compileCode = tb.compile(tb.parse(src))
    val result = compileCode().asInstanceOf[Class[_]]
    val cons = result.getConstructors()(0)
    val instance = cons.newInstance("hemant",3:java.lang.Integer)
    println(instance)
  }
  
  def main(args:Array[String]){
    createCaseClass("name")
    
  }
  
}