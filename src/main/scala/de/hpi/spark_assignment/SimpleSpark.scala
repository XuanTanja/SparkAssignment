package de.hpi.spark_assignment

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level


//SINGLETON
object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("-------------------------------------- Init ---------------------------------------------");


    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkAssignment")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    //32 cores for homework

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    //Serialization and Deserialization
    import spark.implicits._

    //Argument Parsing: https://spark.apache.org/docs/latest/spark-standalone.html
    //https://spark.apache.org/docs/latest/cluster-overview.html

    // java-jar YourAlgorithmName.jar --path TPCH --cores 4
    //TODO: should be dynamic

    val tpch_customer = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_customer.csv")
    val tpch_lineitem = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_lineitem.csv")
    val tpch_nation = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_nation.csv")
    val tpch_orders = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_orders.csv")
    val tpch_part = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_part.csv")
    val tpch_region = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_region.csv")
    val tpch_supplier = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load("data/tpch_supplier.csv")

    tpch_customer.printSchema()
    tpch_customer.show(10)
    tpch_lineitem.show(10)
    tpch_nation.show(10)
    tpch_orders.show(10)
    tpch_part.show(10)
    tpch_region.show(10)
    tpch_supplier.show(10)
    tpch_supplier.printSchema()

  }
}
