package Spark.Example;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        
        SparkConf sc = new SparkConf().setAppName("Exaamples").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        
      JavaRDD<String> txtfile =  jsc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md");
        
//    JavaPairRDD<String, Integer> counts = txtfile.
//  		   flatMap(x -> {
//  			   System.out.println(x); 
//  			    
//  			   return Arrays.asList(x.split(" ")).iterator();})
//  		   
//               .mapToPair(x -> new Tuple2<String,Integer>(x, 1))
//               .reduceByKey((x, y) -> x + y);
//
//        System.out.println(counts.collect());
        
        
        
        
        
        
        
    }
}
