package Spark.Example.IO;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;



public class RddJson {
	 
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf();
		// Set the master to local[2], you can set it to local
		// Note : For setting master, you can also use SparkSession.builder().master()
		// I preferred using config, which provides us more flexibility
		conf.setMaster("local");
		conf.setAppName("s");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
			SparkSession spark =	SparkSession.builder().master("local").config(conf).getOrCreate();
			
			
 
			
		Dataset<Row> f = spark.read()
				.option("multiline", "true")
				.json("/home/hasura/Music/inp.json");
		
		Dataset<Row> g =  f.select("array");
		
		g.write().mode(SaveMode.Overwrite).format("parquet").save("/home/hasura/Music/inp.parquet");
		
		Dataset<Row> k = spark.read().parquet("/home/hasura/Music/inp.parquet");
		k.show();
		
		
		
	
	}

}
