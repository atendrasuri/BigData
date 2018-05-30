package com.sapient;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * @author Bharath
 *
 */
public class WordCountKafka {

	 private static final Pattern SPACE = Pattern.compile(" ");

	  private WordCountKafka() {
	  }

	  public static void main(String[] args) throws Exception {
	    if (args.length < 4) {
	      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
	      System.exit(1);
	    }
	    
	    System.out.println("I m here");

	    //StreamingExamples.setStreamingLogLevels();
	    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaKafkaWordCount");
	    // Create the context with 2 seconds batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

	    int numThreads = Integer.parseInt(args[3]);
	    Map<String, Integer> topicMap = new HashMap<>();
	    String[] topics = args[2].split(",");
	    for (String topic: topics) {
	      topicMap.put(topic, numThreads);
	    }

	    JavaPairReceiverInputDStream<String, String> messages =
	            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
	    System.out.println("Here 0");
	    JavaDStream<String> lines = messages.map(Tuple2::_2);
	    System.out.println("Here 1");
	    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
	        .reduceByKey((i1, i2) -> i1 + i2);
	    System.out.println("Here 2");
	    
	    wordCounts.print();
	    jssc.start();
	    jssc.awaitTermination();
	  }
	}
