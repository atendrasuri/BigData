package com.sapient;


import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
// $example off:schema_merging$
import java.util.Properties;

// $example on:basic_parquet_example$
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
// $example on:schema_merging$
// $example on:json_dataset$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:json_dataset$
// $example off:schema_merging$
// $example off:basic_parquet_example$
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class HealthCareusingSpark {



	public static void main(String[] args) {
		//winutils.exe chmod 777 D:\tmp\hive
		System.setProperty("hadoop.home.dir", "D:\\Hadoop Materials\\");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL data sources example")
				.config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath()).enableHiveSupport()
				.config("spark.local.dir", "/tmp/hive")
				.getOrCreate(); 
		runBasicDataSourceExample(spark);
		/*runBasicParquetExample(spark);
	    runParquetSchemaMergingExample(spark);
	    runJsonDatasetExample(spark);
	    runJdbcDatasetExample(spark);
		 */
		spark.stop();
	}

	private static void runBasicDataSourceExample(SparkSession spark) {
		// TODO Auto-generated method stub
		
		Dataset<Row> usersDF =spark.read().csv("D:\\HadoopTraining\\healthcaredata\\*.csv");
		//System.out.println("df.count()"+usersDF.count());
		
		usersDF = usersDF.toDF("id", "un", "dob", "cont1", "mailId", "cont2", "gender", "disease", "weight");
		//usersDF.write().partitionBy("gender","disease").bucketBy(5, "mailId").format("orc")
		//.saveAsTable("HEALTH_CARE");
		usersDF = spark.sql("select disease, gender ,count(*) from HEALTH_CARE group by disease,gender");
		usersDF.show();
		
		//usersDF.
		
		
		

	}
}
