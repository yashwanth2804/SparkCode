package Spark.Example;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AggFold {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);

		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile("/home/hasura/Music/spark-2.4.0-bin-hadoop2.7/README.md").repartition(10);

		JavaRDD<String> words = file.flatMap(f -> Arrays.asList(f.split(" ")).iterator()).filter(f -> !f.isEmpty());
		JavaPairRDD<String, Integer> wordMap_to_pair = words.mapToPair(f -> new Tuple2<String, Integer>(f, 1));

		// Fold- action

		Tuple2<String, Integer> _Fold = wordMap_to_pair.fold(new Tuple2<String, Integer>("", 0), ((acc, val) -> {
			if (acc._2 > val._2)

				return acc;

			else

				return val;

		}));

		System.out.println(_Fold);

		// FoldBykey

		JavaPairRDD<String, Integer> _Foldbykey = wordMap_to_pair.foldByKey(0, ((acc, val) -> {
			if (acc > val)

				return acc;
			else

				return val;

		}));

		System.out.println(_Foldbykey.collect());
		
		//AggregatebyKey
		
 	JavaPairRDD<String,Integer> _aggreagteBykey = 	wordMap_to_pair.aggregateByKey(
 			0
 				, ( (a,v) -> a+v ) //seq
 				, ( (a,v) -> a+v )); // merge
 
 	 
 	System.out.println(_aggreagteBykey.collect());
 
		
		

	}

}
