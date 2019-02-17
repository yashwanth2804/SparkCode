package Spark.Example;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CombineTf {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	  		
		JavaRDD<String> file = sc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md").repartition(10);
	   
		JavaRDD<String> words = file.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).filter(f -> !f.isEmpty());
		 JavaPairRDD<String,Integer> wordMap_to_pair =  words.mapToPair(f -> new Tuple2<String,Integer>(f,1) );
		 
		 wordMap_to_pair.combineByKey(
				 i -> i, 
				 (a,v) -> (a+v)
				 ,(a,b) -> a+b)
		 .sortByKey()
		 .collect().forEach(System.out::println);
		
	}

}
