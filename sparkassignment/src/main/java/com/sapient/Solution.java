package com.sapient;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class Solution {

	public static void main(String args[]) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> custs = jsc.textFile("C:\\BigData\\custs");
		JavaRDD<String> txns = jsc.textFile("C:\\BigData\\txns");
		System.out.println("hello");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL basic example")
				.getOrCreate();

		
		
		
//		JavaRDD<String> txns1 = txns.flatMap(s -> Arrays.asList(s.split("/n")).iterator());
//		JavaRDD<String> custs1 = custs.flatMap(s -> Arrays.asList(s.split("/n")).iterator());
//
//		JavaRDD<String> reRDD1 = custs1.repartition(2);
//		JavaRDD<String> reRDD2 = txns1.repartition(2);
//
//		JavaPairRDD<Integer, Double> txns2 = reRDD2
//				.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",")[2]), Double.parseDouble(s.split(",")[3])));

		/*JavaPairRDD<Integer, Double> txns3 = txns2.reduceByKey((i1, i2) -> i1 + i2);

		System.out.println(txns3.collect().toString());

		JavaPairRDD<Integer, String> custs3 = reRDD1
				.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",")[0]), s.split(",")[1]));

		System.out.println(custs3.collect().toString());

		JavaPairRDD<Integer, Tuple2<String, Double>> result = custs3.join(txns3);

		result.mapToPair(data -> new Tuple2(data._1() + " " + data._2()._1(), data._2()._2()))
		.mapToPair(data -> data.swap()).sortByKey(false).take(3).stream()
		.forEach(data -> System.out.println(data._1() + "--->" + data._2()));
		System.out.println(result.collect());*/
		
		
		
		JavaRDD<Stats> taxRDD = spark.read().textFile("C:\\BigData\\txns").javaRDD().map(line -> {
			String[] parts = line.split(",");
			Stats person = new Stats();
			person.setId(Integer.parseInt(parts[2]));
			person.setValue(Double.parseDouble(parts[3]));
			return person;
		});

		
		
	}
}
class Stats implements Serializable
{

	private int id;
	//private String name;
	private double value;

	Stats(int id,  double value)
	{
		this.id=id;
		this.value=value;
	}

	public Stats() {
		// TODO Auto-generated constructor stub
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public Stats addData(Stats value)
	{
		return new Stats(id,this.value+value.getValue());
	}


}


