package Spark.Example.Set_Rel;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinCogroup {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	Logger.getLogger("org").setLevel(Level.OFF);
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
	 
		// spark session is interdce
		
	SparkSession spark =   SparkSession.builder().master("local").getOrCreate();
	
		 Dataset<Row> mks = 	spark.read().option("header", "true").
				 option("inferSchema","true")
				 .csv("/home/hasura/Music/marks.csv");
		 Dataset<Row> stu = 	spark.read()
				 .option("inferSchema","true")
				 .option("header", "true").csv("/home/hasura/Music/stu.csv");
		 
		 Dataset<Row> stu1 = 	spark.read()
				 .option("inferSchema","true")
				 .option("header", "true").csv("/home/hasura/Music/stu1.csv");
  
		 
		 stu.join(mks,mks.col("sid")).show();
		 
		 
		 
		
	}

}
