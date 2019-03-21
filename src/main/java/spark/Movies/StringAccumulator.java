package spark.Movies;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

public class StringAccumulator extends AccumulatorV2<String,String> implements Serializable {

	private String _value ="";
	
	public StringAccumulator() {
		// TODO Auto-generated constructor stub
		this("");
	}
	
	
	public StringAccumulator(String initialvalue) {
		// TODO Auto-generated constructor stub
		_value = initialvalue;
	}


	@Override
	public void add(String v) {
		// TODO Auto-generated method stub
		_value = value()+" "+v;
		
	}
 

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		  return (value().length() == 0);
	}

	@Override
	public void merge(AccumulatorV2<String, String> other) {
		// TODO Auto-generated method stub
		add(other.value());
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		_value ="";
		
	}

	@Override
	public String value() {
		// TODO Auto-generated method stub
		return _value;
	}


	@Override
	public AccumulatorV2<String, String> copy() {
		// TODO Auto-generated method stub
		return new StringAccumulator(value());
	}

 
}
