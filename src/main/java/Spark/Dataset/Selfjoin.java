package Spark.Dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Selfjoin {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkSession spark = SparkSession.builder().appName("d").master("local[*]").getOrCreate();
	
		Dataset<Row> ff =  spark.read()
				.option("inferSchema",true)
				.option("header", "true")
				.csv("/home/hasura/Desktop/SparkData/emp.csv");
	
		Dataset<Row> ff1 = ff;
		
		//ff.join(ff1, ff.col("empid").equalTo(ff1.col("mid"))).show();
		
		  ff.as("a")
		 .join(ff.as("b"))
		 .where("a.empid = b.mid")
		 
		 .selectExpr("b.name as employee"," a.name as Employer")
		 
		 .show();
		
		
		//selfJoin(ff).show();
		
		
	}
	  public static Dataset<Row> selfJoin(Dataset<Row> df) {
		    return (df.as("a")).join(df.as("b")).where("a.empid = b.mid");
		  }

}
