package de.hpi.spark_assignment

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._ //This is defined inside of the sql context class (therefore it needs to be imported after initializing spark session)

    val datasets = inputs.map(filename => { //Importing data from csvs
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(filename)
    })

    val columns = datasets.flatMap(data => data.columns.map(data.select(_))) //get columns from dataset

    val cells = columns.map(col => col //entry of column is mapped to column name
     .map(row => (row.get(0).toString, row.schema.fieldNames.head))) //iterate through columns, and get each row of the column (row.get(0)) with the schema header names
    //this is a data set because of .map -> example: [_1: string, _2: string]



    val groupedCellsTuples = cells.reduce((cell1,cell2) => cell1.union(cell2)) //joining 2 cells at a time and calling reduce to combine cells with the lambda expression (function)

    //We need to convert it to dataframe to use agg(collect_set())
    val groupedCellsDataFrame = groupedCellsTuples.toDF("value", "singletonType") //creating dataframe with header


    //https://stackoverflow.com/questions/37737843/aggregating-multiple-columns-with-custom-function-in-spark
    //https://bzhangusc.wordpress.com/2015/03/29/the-column-class/
    //We get columns "value" with groupedCellsDataFrame("value")
    val groupedValueSingletonType = groupedCellsDataFrame //aggregated by values column  and list the singleton types without "singletonType" duplicates
      .groupBy(groupedCellsDataFrame("value")).agg(collect_set(groupedCellsDataFrame("singletonType")).as("aggregatedAttributeSet"))
      //collect_set is used to collect unique values
    //Example: [25508.61,WrappedArray(L_EXTENDEDPRICE, O_TOTALPRICE)]

    //Value is removed (value is just useful for grouping of singletonType names)
    val attributeSets = groupedValueSingletonType
      .select("aggregatedAttributeSet")
      .distinct() //We remove duplicates because we are interested in inclusion dependencies (being sufficiently represented by one list)
      .as[Seq[String]] //We use a list (Seq is a list in scala) of all the strings
    //Example: List(O_ORDERKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY)





  }
}