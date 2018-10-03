package com.scala.practise

abstract class Element {
  def contents: Array[String]
  def width:Int = if(height == 0) 0 else contents(0).length()
  def height:Int = contents.length
  def beside(that: Element):Element = new ArrayElement( for(
      (elem1,elem2) <- this.contents zip that.contents)
    yield (elem1+elem2)
  )
  def above(that:Element):Element = new ArrayElement( this.contents ++ that.contents)
}


class ArrayElement(conts: Array[String]) extends Element {
  val contents = conts
  override def toString() = contents.mkString(",")
}

class LineElement(s:String) extends Element {
  val contents = Array(s)
  override def height:Int = 1
  override def width:Int = s.length
}

class UniformElement(ch:Char,
    override val height:Int,
    override val width:Int) extends Element {
  private val line = ch.toString*width
  val contents = Array.fill(height)(line)
}

  
object Element{
def elem(arr:Array[String]): Element = new ArrayElement(arr)
def elem(str:String):Element = new LineElement(str)
def elem(ch:Char,height:Int,width:Int) = new UniformElement(ch,height,width)
}