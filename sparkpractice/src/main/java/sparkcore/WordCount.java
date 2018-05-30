package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.util.control.Exception;

import java.util.Arrays;
import java.util.List;

public class WordCount {


    public static void main(String args[]){
        System.out.print("Hello");


        SparkConf spark = new SparkConf().setMaster("local").setAppName("WordCount");

        JavaSparkContext jsc = new JavaSparkContext(spark);
        System.out.print("Hello");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
       JavaRDD<Integer> distData = jsc.parallelize(data);
        res=distData.collect();
        System.out.print("Hello");
       System.out.print("total count is ");
    }
}
