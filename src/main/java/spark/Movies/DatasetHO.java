package spark.Movies;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;


public class DatasetHO {

	public static void main(String[] args) {

		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkSession spark = SparkSession.builder().appName("d").master("local[*]").getOrCreate();
		
		
		//Creating StructField 
	 StructField [] sf = new StructField[] {
			 DataTypes.createStructField("id",DataTypes.IntegerType, true),
			 DataTypes.createStructField("name",DataTypes.StringType,true),
			 DataTypes.createStructField("Age",DataTypes.IntegerType, true),
			 DataTypes.createStructField("Friends",DataTypes.IntegerType, true),
	 };
	 
	 StructType st = DataTypes.createStructType(sf);
		
		
		Dataset<Row> ff =  spark.read()
				.schema(st)
				.csv("/home/hasura/Desktop/SparkData/fakefriends.csv");
		 
		//ff.show();
		//ff.filter( ff.col("Age").leq(55)).show();
		// expression
		//ff.filter("Age < 55").show();
		
		//ff
		//.select("*")
	//	.withColumn("id", ff.col("id").multiply(100))
//		.groupBy("Age").count()
//		
//		.withColumnRenamed("count", "Age_counts")
	//	.show();
	
		
		StructField [] sf1 = new StructField[] {
				 DataTypes.createStructField("uid",DataTypes.IntegerType, true),
				 DataTypes.createStructField("mid",DataTypes.IntegerType,true),
				 DataTypes.createStructField("rating",DataTypes.IntegerType, true),
				 DataTypes.createStructField("time",DataTypes.IntegerType, true),
		 };
		 
		 StructType st1 = DataTypes.createStructType(sf1);
		 
		
		Dataset<Row> mv =  spark
				.read()
				 .schema(st1)
				.format("com.databricks.spark.csv")
				.option("delimiter", "\t")
				
				.csv("/home/hasura/Desktop/SparkData/u.data");
		
		
		Dataset<Row> mvI =  spark
				.read()
				 
				.format("com.databricks.spark.csv")
				.option("delimiter", "|")
				
				.csv("/home/hasura/Desktop/SparkData/u.item");
		
		
	Dataset<Row> mvII=	 mvI.select("_c0","_c1")
		.withColumnRenamed("_c0","mid")
		.withColumnRenamed("_c1","name")
		;
	
	
		//mv.show();
		
		//height rated movie 
		
	 
//		mv
//		.groupBy(mv.col("mid"))
//		.avg("rating")
//		//.count()
//		.orderBy(functions.desc("avg(rating)"))
//		// .sort(functions.desc("count"))
//		.join(mvII,"mid")
//		.drop("mid")
//		.show();
		 
		UserDefinedFunction increaserating = udf(
		 		(Integer s) -> s+1,DataTypes.IntegerType
				);
		
		mv.
		//select(increaserating.apply(mv.col("rating")))
		
		withColumn("rating",increaserating.apply(mv.col("rating")))
		.show();
		
		
		
		
		 
		
		
		spark.stop();
	}

}
