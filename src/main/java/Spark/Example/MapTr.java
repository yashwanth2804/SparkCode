package Spark.Example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;


public class MapTr {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//Logger.getLogger("akka").setLevel(Level.OFF);
		Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	 
		
		JavaRDD<String> file = sc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md");
		
		// map
		
		JavaRDD<String> up = file.map(f -> f.trim().toUpperCase());
		System.out.println(up.collect());
		
		// map Arrays
		JavaRDD<List<String>> upArr = file.map(f -> Arrays.asList(f.split(" ")));
		System.out.println(upArr.collect());
				 
		
		
		// filter
		JavaRDD<String> filter_big = up.filter(f -> f.contains("PRO"));
		
		System.out.println(filter_big.collect());
		
		
		// Flatmap
		
				JavaRDD<String> Fup = file.map(f -> f.trim().toUpperCase());
				
				System.out.println(Fup.collect());
				
		//Flatmmap Array
		JavaRDD<String> FupArr = file.flatMap(f ->Arrays.asList(f.split(" ")).iterator());
		System.out.println(FupArr.collect());
		
		
		
	}

}
