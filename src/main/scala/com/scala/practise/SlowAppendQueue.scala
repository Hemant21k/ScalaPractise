package com.scala.practise

import scala.reflect.ClassTag
import scala.reflect.runtime._
import scala.reflect.runtime.universe._


trait Queue[T] {
  def head: T
  def tail: Queue[T]
  def append(x: T): Queue[T]
}

object Queue {

  def apply[T](xs: T*): Queue[T] =
    new QueueImpl[T](xs.toList, Nil)

  private class QueueImpl[T](
      private val leading: List[T],
      private val trailing: List[T]) extends Queue[T] {

    def mirror =
      if (leading.isEmpty)
        new QueueImpl(trailing.reverse, Nil)
      else
        this

    def head: T = mirror.leading.head

    def tail: QueueImpl[T] = {
      val q = mirror
      new QueueImpl(q.leading.tail, q.trailing)
    }

    def append(x: T) =
      new QueueImpl(leading, x :: trailing)
    
  }
}

object test {
  
  def rangeCheck[T:ClassTag](x:T): Option[T] ={
    
    println(x.asInstanceOf[T])
    //println(x.asInstanceOf[Int])
    println(x.isInstanceOf[T])
    Some(x)    
  }
  
  def main(args: Array[String]): Unit = {
    val q = List[Int](1,2,3)
    val h = q.head
    val t = q.tail
    val x = 23
/*    for(qi <- q){
      rangeCheck[Float](x)
    }*/
    
    val ranc = rangeCheck[Float](x)
    if (!ranc.isEmpty){
     println(ranc.get)
     println(typeOf[String])
    }

  }
}
