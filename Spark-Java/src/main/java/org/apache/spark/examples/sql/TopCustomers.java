package org.apache.spark.examples.sql;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.examples.sql.JavaSparkSQLExample.Person;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:create_ds$
// $example off:programmatic_schema$
// $example on:create_ds$
// $example on:schema_inferring$
// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example on:create_ds$
// $example off:programmatic_schema$
// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example off:create_df$
// $example off:run_sql$
// $example off:create_ds$
// $example off:schema_inferring$
// $example off:init_session$
// $example on:programmatic_schema$

public class TopCustomers {

	public static class Customer implements Serializable {

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getLasttname() {
			return lasttname;
		}

		public void setLasttname(String lasttname) {
			this.lasttname = lasttname;
		}

		public String getProfession() {
			return profession;
		}

		public void setProfession(String profession) {
			this.profession = profession;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		String firstname;
		String lasttname;
		String profession;
		int age;
		int custId;

		public int getCustId() {
			return custId;
		}

		public void setCustId(int custId) {
			this.custId = custId;
		}

	}

	public static class Transaction implements Serializable {



		int custId;
		public int getCustId() {
			return custId;
		}

		public void setCustId(int custId) {
			this.custId = custId;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public String getCategory() {
			return category;
		}

		public void setCategory(String category) {
			this.category = category;
		}

		public String getProduct() {
			return product;
		}

		public void setProduct(String product) {
			this.product = product;
		}


		String city;
		String state;
		String category;
		String product;
		String date;
		int serialId;
		public int getSerialId() {
			return serialId;
		}

		public void setSerialId(int serialId) {
			this.serialId = serialId;
		}

		public String getTxnType() {
			return txnType;
		}

		public void setTxnType(String txnType) {
			this.txnType = txnType;
		}


		String txnType;


		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public float getAmount() {
			return amount;
		}

		public void setAmount(float amount) {
			this.amount = amount;
		}

	
		float amount;

	}

	public static void main(String[] args) throws FileNotFoundException {

		System.setErr(new PrintStream("C:\\BigData\\err.log"));
		SparkSession spark = SparkSession.builder().master("local[2]")
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
		
		SparkSession spark1 = SparkSession.builder().master("local[2]")
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		JavaRDD<Customer> cusotmerRDD = spark
				.read()
				.textFile(
						"C:/Hadoop/Content/Classroom/Education/Hadoop/Module4/AdvanceMR_Example/MapJoinProgram/custs")
				.javaRDD().map(line -> {
					String[] parts = line.split(",");
					Customer customer = new Customer();
					customer.setCustId(Integer.parseInt(parts[0].trim()));
					customer.setFirstname(parts[1]);
					customer.setLasttname(parts[2]);
					customer.setAge(Integer.parseInt(parts[3].trim()));
					customer.setProfession(parts[4]);
					return customer;
				});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> customerDF = spark.createDataFrame(cusotmerRDD,
				Customer.class);
		// Register the DataFrame as a temporary view
		customerDF.createOrReplaceGlobalTempView("customers");
		//customerDF.show();

		JavaRDD<Transaction> txnsRDD = spark1
				.read()
				.textFile(
						"C:/Hadoop/Content/Classroom/Education/Hadoop/Module4/AdvanceMR_Example/MapJoinProgram/txns")
				.javaRDD().map(line -> {
					String[] parts = line.split(",");
					Transaction txns = new Transaction();
					txns.setSerialId(Integer.parseInt(parts[0].trim()));
					txns.setDate(parts[1]);
					txns.setCustId(Integer.parseInt(parts[2].trim()));
					txns.setAmount(Float.parseFloat(parts[3].trim()));
					txns.setCategory(parts[4]);
					txns.setProduct(parts[5]);
					txns.setState(parts[6]);
					txns.setCity(parts[7]);
					txns.setTxnType(parts[8]);
					return txns;
				});
		
		Dataset<Row> txnsDf = spark1.createDataFrame(txnsRDD,
				Transaction.class);
		// Register the DataFrame as a temporary view
		txnsDf.createOrReplaceGlobalTempView("transactions");
		
		txnsDf.show();
		
		Dataset<Row> sqlDF = spark.sql("SELECT firstname FROM customers");
		//Dataset<Row> sqlDF = spark.sql("SELECT A.firstname,Count(*),SUM(B.amount) FROM customers A JOIN transactions B ON A.custId=B.custId group by A.firstname order by amount desc limit 3 ");
		sqlDF.show();

	}
}
