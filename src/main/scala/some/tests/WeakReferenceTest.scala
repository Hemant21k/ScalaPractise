package some.tests

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;


/**
 * http://javarevisited.blogspot.in/2014/03/difference-between-weakreference-vs-softreference-phantom-strong-reference-java.html#axzz5CqW2oNGu
 * http://www.programmr.com/blogs/what-every-java-developer-should-know-strong-and-weak-references
 * https://dzone.com/articles/practical-uses-weakreferences */

class WeakReferenceTest {
       var status ="Hi I am active";
       
       def getStatus():String= {
              return status;
       }
       
       def setStatus(status:String)= {
              this.status = status;
       }
       
       
       override def toString():String= {
              "ReferenceExample [status=" + status + "]"
       }
       
       def strongReference()
       {
              val ex = new WeakReferenceTest();
              System.out.println(ex);
       }
       def softReference()
       {
              val ex = new SoftReference[WeakReferenceTest](getRefrence());
              System.out.println("Soft refrence :: " + ex.get());
       }
       
       def weakReference()
       {
              var counter=0;
              val ex = new WeakReference[WeakReferenceTest](getRefrence());
              while(ex.get()!=null)
              {
                     counter += 1;
                     System.gc();
                     System.out.println("Weak reference deleted  after:: " + counter + ex.get());
              }
       }
       def phantomReference() {
              val queue = new ReferenceQueue[WeakReferenceTest]();
              val ex = new PhantomReference[WeakReferenceTest](getRefrence(),queue);
              System.gc();
              queue.remove();
              System.out.println("Phantom reference deleted  after");
       }
       private def getRefrence(): WeakReferenceTest=
       {
              new WeakReferenceTest();
       }
       
}

object MainCaller{
  
  
       def main(args: Array[String]):Unit = {
              val ex = new WeakReferenceTest();
              ex.strongReference();
              ex.softReference();
              ex.weakReference();
              try {
                     ex.phantomReference();
              } catch {
                     // TODO Auto-generated catch block
                case e:InterruptedException =>  println(e.printStackTrace())
                case u:Throwable => println(u.getMessage)
              }
       }
}