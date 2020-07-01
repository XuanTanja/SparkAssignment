package de.hpi.spark_assignment

import java.nio.file.Files

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File

import org.apache.spark.sql

import scala.collection.mutable.ArrayBuffer


//SINGLETON
object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    val usage = "Usage: [--path TPCH] [--Cores Number]"

    if (args.length == 0) {
      println(usage.toString)
      //return 0
    }

    val path = "data" //TODO: this must be passed as argument

    val numCores = "4"

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("-------------------------------------- Init ---------------------------------------------");


    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkAssignment")
      .master("local[" + numCores + "]") // local, with numCores worker cores
    val spark = sparkBuilder.getOrCreate()
    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //Does this have to do with num of cores??

    //32 cores for homework

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    //Serialization and Deserialization
    import spark.implicits._

    //Argument Parsing: https://spark.apache.org/docs/latest/spark-standalone.html
    //https://spark.apache.org/docs/latest/cluster-overview.html

    // java-jar YourAlgorithmName.jar --path TPCH --cores 4

    val dataArrayPaths: Array[File] = (new File(path + "/"))
      .listFiles
      .filter(_.toString.endsWith(".csv"))

    //for (name <- dataArrayPaths) {println(name.toString)}
    //scala map function --> replace loop with map funtion, each path to a dataframe
    var dataFrameArray = ArrayBuffer[sql.DataFrame]() //list of dataframes

    for (dataPath <- dataArrayPaths){
      var aux = spark
      .read
      .option("inferSchema", "true") //to maintain variable types (int, string, etc)
      .option("header", "true")
      .format("csv")
      .option("delimiter", ";")
      .load(dataPath.toString)

      dataFrameArray += aux //add each dataframe to list of dataframes
    }

    for (dataFrame<-dataFrameArray) {
      dataFrame.printSchema()
      dataFrame.show(2)
    }

    //-> Call functions; DataFrame = Spark with SQL; how to transfrom data ->


  }
}
