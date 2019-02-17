package Spark.Example;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReduceTF {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	  		
		JavaRDD<String> file = sc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md").repartition(10);
	   
		JavaRDD<String> words = file.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).filter(f -> !f.isEmpty());
		 JavaPairRDD<String,Integer> wordMap_to_pair =  words.mapToPair(f -> new Tuple2<String,Integer>(f,1) );
	
		 System.out.println(wordMap_to_pair.count());
		 
		 // reduce - action 
		 Tuple2<String,Integer> _reduce =  wordMap_to_pair.reduce((a1,a2)-> {
			 return new Tuple2<String, Integer>("Total count",a1._2+a2._2);
			 
		 });
		 System.out.println(_reduce);
		 
		 // Reduceby 
		 
		JavaPairRDD<String,Integer> _reducebyKey =  wordMap_to_pair.reduceByKey((a,b) -> a+b);
		 System.out.println(_reducebyKey.collect());
		 
		 Map<String,Long>d =  wordMap_to_pair.countByKey();
		 
		 for( Map.Entry f : d.entrySet())
			 System.out.println(f.getKey()+" "+f.getValue());
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
	}

}
