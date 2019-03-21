package spark.Movies;
 
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.*;

import org.apache.spark.*	;

 

public class Accumulators {

	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		 
		LongAccumulator   lac	= sc.sc().longAccumulator("LAC");
	//	sc.sc().register(lac, "lac");
		
		DoubleAccumulator dac = sc.sc().doubleAccumulator("DAC");
	//	sc.sc().register(dac, "dac1");
	    CollectionAccumulator<String> cac =	sc.sc().collectionAccumulator();
	 //   sc.sc().register(cac, "cac"); 
		StringAccumulator heightValues = new StringAccumulator();
		sc.sc().register(heightValues);


		JavaRDD<String> cu =  sc.textFile("/home/hasura/Desktop/SparkData/customer-orders.csv");
		
		cu.map(f -> {
			
			lac.add(1);
	 		dac.add(1);
		 	heightValues.add("new");
		 	cac.add("s");
			
			return f;
		}).collect();
		
		System.out.println(lac);
		System.out.println(dac);
		System.out.println(heightValues);
		System.out.println(cac);
		
	}

}
