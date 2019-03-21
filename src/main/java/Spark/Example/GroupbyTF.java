package Spark.Example;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class GroupbyTF {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
			
			SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
			JavaSparkContext sc = new JavaSparkContext(conf);
		  		
			JavaRDD<String> file = sc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md").repartition(10);
		   
			JavaRDD<String> words = file.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).filter(f -> !f.isEmpty());
			 JavaPairRDD<String,Integer> wordMap_to_pair =  words.mapToPair(f -> new Tuple2<String,Integer>(f,1) );
			 
			
			 
			 
			
			 /// GroupBy  
			 
		JavaPairRDD<Object,Iterable<Tuple2<String,Integer>>> _gruopBy = wordMap_to_pair.groupBy(f -> f._1);
			 
		System.out.println(_gruopBy.collect());
		
		//GroupBy key
		
	JavaPairRDD<String,Iterable<Integer>> _groupByKey = wordMap_to_pair.groupByKey();
			 
			 
			 System.out.println(_groupByKey.collect());
			 
			 
			 
			
		// JavaPairRDD<Object,Iterable<String>> res = words.groupBy(f -> f.charAt(0));
		  
		 
		//  Map<Object, Iterable<String>> hh = res.collectAsMap();
		  
//		 hh.forEach( (k,v) -> {
//			 
//			 System.out.println(k+"     "+v);
//		 });
		 
		 
		 
 	 _groupByKey
 		 .mapValues( v -> Iterables.size(v))
 	 .collect() 
 		 .forEach(f -> System.out.println(f));
		  
		 

	}

}
