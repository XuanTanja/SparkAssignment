package de.hpi.spark_assignment
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File


object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {
    //Argument Parsing: https://stackoverflow.com/questions/2315912/best-way-to-parse-command-line-parameters
    //Spark cluster documentation: https://spark.apache.org/docs/latest/cluster-overview.html

    // java-jar SimpleSpark.jar --path data --cores 4

    //TODO: hacer con length == 0 que use standard values de 32 cores

    val usage = "Usage: --path [folderDataPath] --cores [numCores]"

    if (args.length == 0 ||args.length == 4){
      //Default values if args.length == 0
      var path = "./TPCH" //Default folder path of csv files
      var numCores = 4

      if (args.length > 0){
        if (args(0).equals("--path") && args(2).equals("--cores")){
          //Run program
          try {
            path = args(1)
            numCores = args(3).toInt
          }catch {
            case error: NumberFormatException => {
              println("--cores argument is not an integer")
              return 0
            }

          }
        }
        else {
          print(usage)
          return 0
        }
      }

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      println("------------------------------------ Initialize -------------------------------------------");

      // Create a SparkSession to work with Spark (This is where tasks are distributed to Executors or workers)
      val sparkBuilder = SparkSession
        .builder()
        .appName("SparkAssignment")
        .master("local[" + numCores + "]") // local, with numCores worker cores
      val spark = sparkBuilder.getOrCreate()

      // Set the default number of shuffle partitions (the default is 200)
      //Shuffle partitions are the number of partitions that each data frame is divided and handled by an available core
      spark.conf.set("spark.sql.shuffle.partitions", "8") //Does this have to do with num of cores??

      //Get all csv files from folder
      val dataArrayPaths: Array[File] = (new File(path + "/"))
        .listFiles
        .filter(_.toString.endsWith(".csv"))

      val listStringPaths = dataArrayPaths //convert them to a list
        .map(_.toString).toList

      //Call Sindy to work on csv's, passing in spark session
      Sindy.discoverINDs(listStringPaths, spark)

    }
    else{
      println(usage)
    }

  }
}