package spark.Movies;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

public class Rating {

	public static void main(String[] args) {
	//	  TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName("Rating").setMaster("local");
		
		JavaSparkContext sc = new  JavaSparkContext(conf);
		
	JavaRDD<String> file = 	sc.textFile("/home/hasura/Desktop/SparkData/u.data");
		
 	 	file
 	  	.map(f -> Arrays.asList(f.split("\t")).iterator()).foreach(f -> System.out.println(f));
// 	 	.map(f -> f.toString().split("\t")[2])
// 	 	 .flatMap(f -> Arrays.asList(f.split("\t")).iterator())
// 	 	.mapToPair(f -> new Tuple2<String,Integer>(f, 1))
// 	 	.reduceByKey((a,b) -> a+b)
// 	 	.foreach(f -> System.out.println(f));
// 	 	;
//	 	 .foreach(f -> System.out.println(f));
//	 	
	 	// friends mapValues
	 	
 	   JavaRDD<String> fk =  sc.textFile("/home/hasura/Desktop/SparkData/fakefriends.csv");
 	   JavaPairRDD<Integer,Tuple2<Integer,Integer>> gh = 
 			   fk  
 	    .mapToPair(f ->{
 	    		int age = Integer.parseInt(f.split(",")[2]);
 	    		int count = Integer.parseInt(f.split(",")[3]);
 	    		
 	    	return  new Tuple2<Integer, Integer>(age, count);
 	    })
 	  
 	    .mapValues(f -> new Tuple2<Integer,Integer>(f, 1))
 	   
 	    ;
 	    
 	   JavaPairRDD<Integer,Tuple2<Integer,Integer>> TF =   gh.reduceByKey((x,y) -> {
 		   return new Tuple2<Integer,Integer> (x._1+y._1,x._2+y._2);
 	   } );
 	   
 	   TF.mapValues(f -> {
 		   return f._1/f._2;
 	   }).sortByKey().foreach(f -> System.out.println(f));
 	   
 	  
	   
	    gh.collect().forEach(f -> System.out.println(f));
	   
//	    .reduceByKey((a,b) -> ( a._1+b._1 , a._2+b._2 ));
//	    
//	    .foreach(f -> System.out.println(f));
//	    
//	     .map(f -> f.toString().split(","))
//	 
 	 JavaRDD<String> _18 =  sc.textFile("/home/hasura/Desktop/SparkData/1800.csv");
 	   
 	 JavaPairRDD<String, Integer>  jk = 
 			_18
 	 	.map(f -> {
 	 		String fields[] = f.toString().split(",");
 	 		String sid = fields[0];
 	 		 String time = fields[1];
 	 		String etypr = fields[2];
 	 		int temp = Integer.parseInt(fields[3]);
 	 		
 	 		return  new Tuple3<String,String,Integer> (sid,etypr,temp);
 	 		
 	 	}) .filter(f -> {
  		return f._2().equals("TMIN");
  	})
 	 		 	.mapToPair(f -> new Tuple2<String,Integer>(f._1(),f._3()))
 	 		 	.reduceByKey((a,b) -> {
 	 		 		if(a < b) return a; else return b;
 	 		 	})
 	 		 	.sortByKey()  ;
 	
	 	 	
	 	
		
	} 
	

}