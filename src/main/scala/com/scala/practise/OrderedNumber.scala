package com.scala.practise

class OrderedNumber(val mantissa:Float,val expont: Int) extends Ordered[OrderedNumber] {
  def compare(that:OrderedNumber):Int = Math.round((this.mantissa*this.expont) - (that.mantissa*that.expont))
  override def toString:String = "mantissa: " + this.mantissa +"\n" + "exponent: "+ this.expont
}

object OrderedNumber{
  
  val num1 = new OrderedNumber(3,78)
  val num2 = new OrderedNumber(7,89)
  
 def main(args:Array[String]){
    
    val result = if (num1 >= num2) num1 else num2
    
    println("greater among the nums is below num with: " + "\n" +result)
  }
  
}