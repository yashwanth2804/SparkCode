package spark.Movies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.base.Joiner;

import com.fasterxml.jackson.module.scala.util.Strings;

import javassist.expr.Instanceof;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import scala.Tuple2;

public class Combi {

	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		
		
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	  		
		JavaRDD<String> file = sc.textFile("/home/hasura/Videos/fun.csv");
		
	JavaRDD <Tuple2<Integer,Integer>> g1 =
			file.map(f -> {
		 
			
			String[] line = f.split(" ");
			
			List<String> list = Arrays.asList(line);
			 List<Tuple2<Integer,Integer>> d = new ArrayList<Tuple2<Integer,Integer>>() ;
			for(String k : list) {
				for(String l : list) {
					int K = Integer.parseInt(k);
					int L = Integer.parseInt(l);
					if(K != L) {
				 
						d.add(new Tuple2<Integer, Integer>(K, L));
					}
						
				}
			}
			
			 
			return d;
		})
		.flatMap(f -> f.iterator())	;
	
	JavaPairRDD<Integer, Iterable<Integer>> g2 = g1.mapToPair(f -> {
		 return new Tuple2<Integer,Integer>(f._1,f._2);
	})
	.groupByKey()
	.mapValues(f -> {
		
		List<Integer> Target = new ArrayList<Integer>();
		 f.forEach(Target::add);
		 
		 
		  List<Integer> listWithoutDuplicates = Target.stream().distinct().collect(Collectors.toList());
	      
		 
		return listWithoutDuplicates;
	});
	  
		  g2.map(f -> {
			 
			 int key = f._1;
			 
			List<Integer> h = StreamSupport.stream(f._2.spliterator(), false).collect(Collectors.toList());
			String s = Joiner.on(' ').join(h);
			 
			  String j = key+"\t"+s;
			 return j;
		 })
		  .saveAsTextFile("/home/hasura/Videos/fun1.tsv");
	
	}

}
