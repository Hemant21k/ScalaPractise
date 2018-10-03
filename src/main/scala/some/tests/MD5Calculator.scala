package some.tests

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.digest.DigestUtils;


object MD5Calculator extends App {
  
  //val logger = Logger.getLogger(classOf[MD5Calculator].getName());
  
      def checkSumApacheCommons(file:String){
        var checksum = "";
        try {  
            checksum = DigestUtils.md5Hex(new FileInputStream(file));
        } 
        
        catch {
          case ex:IOException => println("error in reading file") //logger.log(Level.SEVERE, null, ex);
          case u:Throwable => println(u.getMessage)
        }
        println(checksum)
    }
      
      println(checkSumApacheCommons("D:\\caseclass.txt"))

}