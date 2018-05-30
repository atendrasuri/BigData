package com.sapient.spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext jsc= new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("F:\\BigDataWorkspace\\project_data");

		JavaRDD<String> line = lines.flatMap(f-> Arrays.asList(f.split(",")).iterator());
		JavaPairRDD<String, Integer> words = line.mapToPair(word-> new Tuple2<>(word,1));

		JavaPairRDD<String, Integer> counts = words.reduceByKey((x,y)->x+y);

		counts.foreach(d->System.out.println(d._1+"-->"+d._2()));



	}

}
