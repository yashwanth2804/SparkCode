package spark.Movies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import scala.Tuple4;

public class Movie {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName("Rating").setMaster("local");
		
		JavaSparkContext sc = new  JavaSparkContext(conf);
		
		
		////Map movie ID 
		JavaRDD<String> cuItem =  sc.textFile("/home/hasura/Desktop/SparkData/u.item");
		
		Map<Integer,String> mp = new HashMap<Integer, String>();
		
		
		
		 JavaPairRDD<Integer,String>  itemss =	
		 
		cuItem.mapToPair( f -> {
		 	String h []= f.split("\\|");
		 
			return new Tuple2<>(Integer.parseInt(h[0]),h[1]) ;
		});
		Map<Integer,String> ggg=  itemss.collectAsMap();
		
//		
//		for(Map.Entry<Integer,String> gr : ggg.entrySet()) {
//			System.out.println(gr.getKey() +"  "+gr.getValue());
//			
//		}
		
	 Broadcast<Map<Integer, String>> bsc = sc.broadcast(ggg);
		 
		 
		
		JavaRDD<String> cu =  sc.textFile("/home/hasura/Desktop/SparkData/u.data");
		
		 // uid mid rat time
		// List<Tuple2<Integer, Integer>>  sortByRating = 	cu.map(f -> {
	JavaRDD<Tuple4<Integer,Integer,Integer,Float>>  allFildes = 	cu.map(f -> {
			String lines [] = f.split("\t");
			int mid = Integer.parseInt(lines[0]);
			int uid = Integer.parseInt(lines[1]);
			int rat = Integer.parseInt(lines[2]);
			float timestamp = Float.parseFloat(lines[3]);
			
			return new Tuple4<>(mid,uid,rat,timestamp);
			 
		});
		
	List<Tuple2<String, Integer>>  sortByRating  = allFildes.
			
			mapToPair( f ->  {
				
				 
				return new Tuple2<>(bsc.value().get(f._2()),f._3());
			}	)
		.mapValues(f -> new Tuple2<>(f,1))
		//.reduceByKey((a,b) -> a+b)
		.reduceByKey((a,b)-> {
			return new Tuple2<>(a._1+b._1,a._2+b._2);
		})
		//avg rating 
		.mapValues(f -> {
			
			return  f._1/f._2();
		})
		.map(f -> new Tuple2<>(f._1,f._2))
		.sortBy(f -> f._2, false,1) 
		.collect();
	
 
//		
//		 for(Tuple2<Integer,Integer> g : sortByRating) {
//			 System.out.println(g);
//		 }
		 
		 System.out.println("--------most rated------------");
		 /// Most rated movie 
		 
		 allFildes
		 .mapToPair(f -> {
			 return new Tuple2<>(f._2(),1);
		 })
		 .reduceByKey((a,b) -> a+b)
		 .map(f -> new Tuple2<>(bsc.value().get(f._1()),f._2))
		 .sortBy(f -> f._2, false, 10)
		 .foreach(f -> System.out.println(f));
		 
		 
		 System.out.println("-------most Active using------");
		 allFildes
		 .mapToPair(f -> {
			 return new Tuple2<>(f._1(),1);
		 })
		 .reduceByKey((a,b) -> a+b)
		 .map(f -> new Tuple2<>(f._1,f._2))
		 .sortBy(f -> f._2, false, 10)
		 .foreach(f -> System.out.println(f));
		 
		 
	}

}
