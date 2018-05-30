package com.sapient;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.sapient.JavaSparkSQLExample.Person;

public class SparkSql2 {

	public static void main(String[] args) throws AnalysisException {

		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL basic example")
				.getOrCreate();

		Dataset<Row> customerDataSet =	getCustomerRDD(spark);
		Dataset<Row> transDataSet = getTransactionRDD(spark);
		Dataset<Row> results = spark.sql(
				"select sum(t.rate) as rate, count(*), c.name from tax t,cust c where t.id=c.id group by c.name order by rate desc limit 3");
	
		results.show();
		System.out.println();
		

	}

	public static Dataset<Row>  getTransactionRDD(SparkSession spark) {
		JavaRDD<Tax> taxRDD = spark.read().textFile("C:\\BigData\\txns").javaRDD().map(line -> {
			String[] parts = line.split(",");
			Tax person = new Tax();
			person.setId(Integer.parseInt(parts[2]));
			person.setRate(Double.parseDouble(parts[3]));
			return person;
		});

		Dataset<Row> taxsDF = spark.createDataFrame(taxRDD, Tax.class);
		taxsDF.createOrReplaceTempView("tax");
		return taxsDF;
		
	}

	public static Dataset<Row>  getCustomerRDD(SparkSession spark) {
		JavaRDD<Customer> peopleRDD = spark.read().textFile("C:\\BigData\\custs").javaRDD().map(line -> {
			String[] parts = line.split(",");
			Customer person = new Customer();
			person.name = parts[1] + " " + parts[2];
			person.id = Integer.parseInt(parts[0]);
			return person;
		});
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Customer.class);
		peopleDF.createOrReplaceTempView("cust");
		peopleDF.printSchema();
		return peopleDF;
	}

}
