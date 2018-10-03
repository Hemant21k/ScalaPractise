package some.tests

/**
 * 
 */

class DynamicClassExample(str:String) {
  
  /**
   * https://www.tylerburton.ca/2011/06/dynamically-load-a-jar-at-runtime/
   * https://stackoverflow.com/questions/8868394/scala-dynamic-class-loading-class-a-can-not-be-cast-to-class-a/8868537#8868537
   * https://stackoverflow.com/questions/3039822/how-do-i-call-a-scala-object-method-using-reflection
   * https://stackoverflow.com/questions/8867766/scala-dynamic-object-class-loading?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
   */
    def callHi()={
    println("sayHi" + str)
  }
}