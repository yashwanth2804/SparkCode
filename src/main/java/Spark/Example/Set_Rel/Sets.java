package Spark.Example.Set_Rel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Sets {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String [] words_ = {"A","B","C","D","C"};
		String [] words_1 = {"A","D","R"};
		
		List<String> words1 = Arrays.asList(words_);
		List<String> words2 = Arrays.asList(words_1);
		
	JavaRDD<String> wordsRdd_1 = 	sc.parallelize(words1);
	JavaRDD<String> wordsRdd_2 = 	sc.parallelize(words2);
	
	//Union
	
	wordsRdd_1.union(wordsRdd_2).collect().forEach(f -> System.out.println(f));
	System.out.println();
	//interseaction
	wordsRdd_1.intersection(wordsRdd_2).collect().forEach(f -> System.out.println(f));
	System.out.println();
	
	// Subtract 
	wordsRdd_1.subtract(wordsRdd_2).collect().forEach(f -> System.out.println(f));
	System.out.println();
	
	wordsRdd_1.distinct().collect().forEach(f -> System.out.println(f)); 
		 
	}

}
