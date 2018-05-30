package org.apache.spark.examples;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class Accumulator {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster(
				"local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//		JavaRDD<String> rdd = jsc
//				.textFile("C:\\codebase\\scala-project\\input data\\movies_data_2");
		
		JavaRDD<String> rdd = jsc
				.textFile("C:\\BigData\\Sales.txt");
		//org.apache.spark.Accumulator<Integer> acc = jsc.accumulator(0, "Count");
		
		LongAccumulator accum = jsc.sc().longAccumulator();
		
		JavaRDD<Boolean> parseddata = rdd.map(input -> {
			String[] columns = input.split(",");
			if(columns.length == 4){
				return true;
			}
			else {
				accum.add(1);
				return false;
			}
			
		});
		
		
		for (Boolean isProper : parseddata.collect()) {
			System.out.println(isProper);	
			
		}
		
		System.out.println(accum.value());
	}

}
