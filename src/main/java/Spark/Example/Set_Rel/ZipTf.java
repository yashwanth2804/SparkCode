package Spark.Example.Set_Rel;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sun.tools.javac.util.List;

public class ZipTf {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	  		
		
		//ZIP 
		
		JavaRDD<String> DS1 = sc.textFile("/home/hasura/Music/ds1.md",1);
		
		JavaRDD<String> DS2 = sc.textFile("/home/hasura/Music/ds2.md");
		
		//DS1.zip(DS2).collect().forEach(f -> System.out.println(f));
		
		//ZIPPARTION
		
		DS1.zipPartitions(DS2,  
				(a,b) -> {
				
			
		 ArrayList<String>  lst = new ArrayList<String>();
		 
			while(a.hasNext() && b.hasNext()) {
				lst.add(a.next()+" --- "+b.next());
			}
			return Arrays.asList(lst).iterator();
			
		} ).collect().forEach(f -> System.out.println(f))
		;
		
		
		//ZipWithIndex
		
		DS1.zipWithUniqueId().collect().forEach(f -> System.out.println(f));
		
		
	   
	}

}
