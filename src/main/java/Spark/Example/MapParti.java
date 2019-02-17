package Spark.Example;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapParti {

 
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	 
		
		JavaRDD<String> file = sc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md").repartition(10);
	   
		//Mappartition 
		
		JavaRDD<String> g = file.mapPartitions( parti ->  Arrays.asList(parti.next()).iterator() );
		
		System.out.println(g.collect());
		// MapPartion with index
		
		
		// total no of partiotions 
		
		System.out.println(file.getNumPartitions());
		
		JavaRDD<String> h = file.mapPartitionsWithIndex( (i,f) -> {
		 System.out.println(i);
		 return Arrays.asList(f.next()).iterator();
	 },true );
		
	 System.out.println(h.collect());
	
		
		
	}

}
