package org.apache.spark.examples;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

public class Boardcast {
	private static final Pattern COMMA = Pattern.compile(",");
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster(
				"local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = jsc
				.textFile("C:\\BigData\\Sales.txt");
		//org.apache.spark.Accumulator<Integer> acc = jsc.accumulator(0, "Count");
		
		String customerDataPath= "C:\\BigData\\customers.txt";
		
		
		Map<String, String> customerMap = null;
		try {
			customerMap = parseCustomerData(customerDataPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Broadcast<Map<String,String>> broadcastVar = jsc.broadcast(customerMap);
		
		JavaRDD<String> output = rdd.map(line -> {
			String[] columns = line.split(",");
			String custname = broadcastVar.value().get(columns[1]);
			return custname;
			
		});
		
		for (String name : output.collect()) {
			System.out.println(name);	
			
		}
		
	}
	
	public static Map<String,String> parseCustomerData(String input) throws IOException{
		
		Map<String,String> custIdNameMap = new HashMap<String,String>();
		String line= null;
		BufferedReader reader = new BufferedReader(new FileReader(new File(input)));
		
		try {
			while((line = reader.readLine()) != null){
				//line = reader.readLine();
				String[] columns = line.split(",");
				custIdNameMap.put(columns[0], columns[1]);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		reader.close();
		return custIdNameMap;
	}

}
