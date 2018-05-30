package com.sapient;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import scala.collection.parallel.ParIterableLike.Partition;

public class NYSEAssignment {

	public static void main(String args[])throws Exception
	{
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL basic example")
				.getOrCreate();

		JavaRDD<Stock> stockRDD = spark.read().textFile("F:\\HADOOPWORKSPACE\\NYSE.csv").javaRDD().map(line -> {
			String[] parts = line.split(",");
			Stock stock= new Stock();
			stock.setName(parts[1]);
			stock.setStock(Double.parseDouble(parts[6]));
			return stock;
		});
		
	JavaPairRDD<String, Double> sortedStockRDD = stockRDD.mapToPair(stock -> new Tuple2<>(stock.getName(), stock.getStock()))
		.reduceByKey((x,y) -> x+y,10)
		.mapToPair(data -> data.swap())
		.sortByKey(false)
		.mapToPair(data -> data.swap());
	
	sortedStockRDD.collect().forEach(data -> System.out.println(data._1()+""+data._2()));
	//sortedStockRDD.saveAsHadoopFile("hdfs://sandbox.hortonworks.com:8020/stock", Text.class, DoubleWritable.class, TextOutputFormat.class);
	
	JavaPairRDD<String, Iterable<Double>> groupedStockRDD = sortedStockRDD.groupByKey(new CharRangePartition(6));
	
	groupedStockRDD.saveAsHadoopFile("hdfs://sandbox.hortonworks.com:8020/StockGroup", Text.class, DoubleWritable.class, TextOutputFormat.class);
	

	}

}


