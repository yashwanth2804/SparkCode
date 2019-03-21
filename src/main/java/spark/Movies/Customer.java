package spark.Movies;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

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

public class Customer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName("Rating").setMaster("local");
		
		JavaSparkContext sc = new  JavaSparkContext(conf);
		
		JavaRDD<String> cu =  sc.textFile("/home/hasura/Desktop/SparkData/customer-orders.csv");
		 
		 JavaRDD<Tuple3<Integer,Integer,Float>> bn = 	cu.map(
					f -> {
						
						String h[] = f.split(",");
						
						int cid = Integer.parseInt(h[0]);
						int iid = Integer.parseInt(h[1]);
						float amt = Float.parseFloat(h[2]);
						
						return new Tuple3<>(cid,iid,amt);
						 
					}
					);
			
			
		 
			 
			//.foreach(f -> System.out.println(f));
			System.out.println("-----------------order which order got more sales---------------------");
		    JavaRDD<Tuple2<Integer,Float>> bb = bn.mapToPair(f -> {
				
				return new Tuple2<>(f._2(),f._3());
			})
			.reduceByKey((a,b) -> a+b)
			.map(f -> {
				return new Tuple2<Integer,Float>(f._1,f._2);
			});
		 	;
		 	bb.sortBy(f -> f._2, false,1)
		 	.saveAsTextFile("/home/hasura/Desktop/SparkData/orderid.txt");;
		 	 
		 	System.out.println("==========sort by most paid users========");
		  
		 	 bn
		 	 .mapToPair(f -> {
		 		 return new Tuple2<Integer,Float>(f._1(),f._3());
		 	 })
		 	 .reduceByKey((a,b) -> a+b)
		 	 .map(f -> {
		 		 return new Tuple2<Integer,Float>(f._1,f._2);
		 	 })
		 	 .sortBy(f -> f._2,true,1)
		 	.saveAsTextFile("/home/hasura/Desktop/SparkData/cust.txt");
		 	 ;
		 	 
		 	 
		 	
		 	
		 	
		 	
		 	 

	}

}
