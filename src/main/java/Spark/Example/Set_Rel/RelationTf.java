package Spark.Example.Set_Rel;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class RelationTf {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> file_1 = sc.textFile("/home/hasura/Music/file_1.md");
		JavaRDD<String> file_2 = sc.textFile("/home/hasura/Music/file_2.md");
		JavaRDD<String> file_3 = sc.textFile("/home/hasura/Music/file_3.md").repartition(10);
		   
		JavaRDD<String> words_1 = file_1.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).map(f -> f.toUpperCase()).filter(f -> !f.isEmpty());
		JavaRDD<String> words_2 = file_2.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).map(f -> f.toUpperCase()).filter(f -> !f.isEmpty());
		JavaRDD<String> words_3 = file_3.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).map(f -> f.toUpperCase()).filter(f -> !f.isEmpty());
		
		JavaPairRDD<String,Integer> wordMap_to_pair_file_1 =  words_1.mapToPair(f -> new Tuple2<String,Integer>(f,1) );
		
		JavaPairRDD<String,Integer> wordMap_to_pair_file_2 =  words_2.mapToPair(f -> new Tuple2<String,Integer>(f,1) );
		
		JavaPairRDD<String,Integer> wordMap_to_pair_file_3 =  words_3.mapToPair(f -> new Tuple2<String,Integer>(f,1) );
		
		
		
 	wordMap_to_pair_file_1.join(wordMap_to_pair_file_2).collect().forEach(f -> System.out.println(f));
		
		// cogroup is like full outer join , where if key present in either of records , and get flattend iterable output
		
	 JavaPairRDD<String,Tuple2<Iterable<Integer>,Iterable<Integer>>> _cogroupp = wordMap_to_pair_file_1
			 .cogroup(wordMap_to_pair_file_2);
	 	
	 System.out.println(_cogroupp.collect());
	 
	 //LeftOuterJoin
	 System.out.println();
	 
	 wordMap_to_pair_file_1.leftOuterJoin(wordMap_to_pair_file_2).collect().forEach(f -> System.out.println(f));
	 wordMap_to_pair_file_1.rightOuterJoin(wordMap_to_pair_file_2).collect().forEach(f -> System.out.println(f));
	 
	 System.out.println();
	 		
	
		
		 
		
	}

}
