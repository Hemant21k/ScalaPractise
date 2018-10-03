package some.tests

import scala.io.Source
import scala.util.{Try,Success,Failure}
import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.util.{Try,Success,Failure}
import java.net.URL
import java.net.MalformedURLException
import java.io.FileNotFoundException
/**https://www.cakesolutions.net/teamblogs/error-handling-pitfalls-in-scala
 * https://www.cakesolutions.net/teamblogs/ways-to-pattern-match-generic-types-in-scala
 */

object Testsd extends App {

/*    def readTextFile(filename: String): Try[List[String]] = {
        Try(Source.fromFile(filename).getLines.toList)
    }

    val filename = "/etc/passwd"
    
    readTextFile(filename) match {
        case Success(lines) => lines.foreach(println)
        case Failure(f) => println(f)
    }*/
  
  
  
  class Foo { def bar(x: Int) = x }
  
  val foo = new Foo
 
def dynamicMethodInvocation[T](obj:Object,methodName:String,methodParam:T)={
    /**https://stackoverflow.com/questions/11062166/dynamic-method-invocation-with-new-scala-reflection-api*/
  val mirror = ru.runtimeMirror(getClass.getClassLoader).reflect(obj)
  val methodSymbol = mirror.symbol.typeSignature.member(ru.newTermName(methodName))
  val methodInstance = mirror.reflectMethod(methodSymbol.asMethod)
  methodInstance(methodParam)
}
  
  println(dynamicMethodInvocation[Int](foo,"bar",56))
  
def evaluateX(srt1:String):String= {
/*val array = "array"
val string = "string"*/
srt1 match{
case "array" => "aray"
case "string" => "string"
case _ => throw new Exception("unknown type in evaluateX method")
}
}

def evaluateFinal()={
  try{
    evaluateX("hi")
  }
  catch{
    case e:Exception => println("exception caught: " + e)
    case _:Throwable => println("exx")
  }
}

//evaluateFinal()

def addHello(str:String) = str.toInt

def addGreet(st:Int):Try[String] = Try(st.asInstanceOf[String])

def addOneMore(st1:String): Try[String] = Try(st1+"onemore")

val result: Try[String] = Try(addHello("2")).flatMap(addGreet(_))

result match{
  case o @ Failure(_) => println("new exception");o
  case Success(r) => r
}

//println(result)

def parseURL(url: String): Try[URL] = Try(new URL(url))


def getURLContent(url: String): Try[Iterator[String]] =
  for {
    url <- parseURL(url)
    connection <- Try(url.openConnection())
    is <- Try(connection.getInputStream)
    source = Source.fromInputStream(is)
  } yield source.getLines()

  val content = getURLContent("garbage") recover {
  case e: FileNotFoundException => Iterator("Requested page does not exist")
  case e: MalformedURLException => Iterator("Please make sure to enter a valid URL")
  case _ => Iterator("An unexpected error has occurred. We are so sorry!")
}
  
  println(content.get.foreach(print))
}