
package org.apache.spark.examples;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public final class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");


	public static void main(String[] args) throws Exception {
		//System.out.println("Nothing");
		System.setErr(new PrintStream("C:\\BigData\\err.log"));
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
				.builder().master("local")
				.appName("JavaWordCount")
				.getOrCreate();

		//SparkContext jsc = spark.sparkContext();
		//jsc.setCheckpointDir("C:\\checkpoint2");

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		//words.persist(StorageLevel.MEMORY_AND_DISK_SER_2());

		//System.out.println("::"+words.collect().toString());
		//JavaRDD<String> fltwords = words.filter( s -> s.contains("hadoop"));
		JavaRDD<String> reRDD= words.repartition(4);

		System.out.println(reRDD.count());

		JavaPairRDD<String, Integer> ones = reRDD.mapToPair(s -> new Tuple2<>(s, 1));

		//ones.checkpoint();

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		counts.saveAsTextFile("D:\\spark_out1");

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println("-----"+tuple._1() + ": " + tuple._2());
		}
		Thread.sleep(3000);
	   //spark.stop();
	}
}
