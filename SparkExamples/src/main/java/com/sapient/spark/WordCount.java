/**
 * 
 */
package com.sapient.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;



/**
 * @author aku375
 *
 */
public class WordCount {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		
        SparkSession spark = SparkSession.builder().master("local").appName("JavaWordCount").getOrCreate();

   

        JavaRDD<String> baseRDD = spark.read().textFile(args[0]).javaRDD();

         baseRDD.flatMap(lines -> Arrays.asList(lines.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1))
                                        .reduceByKey((x, y) -> x + y).saveAsTextFile(args[1]);

  
        
        Thread.sleep(50000000);
       


	  }

	

}
