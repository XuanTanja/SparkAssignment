package de.hpi.spark_assignment

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._ //This is defined inside of the sql context class (therefore it needs to be imported after initializing spark session)

    //General idea: make use of data frames to modify and group cells, then convert to array/strings


    val datasets = inputs.map(filename => { //Importing data from csv's
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
      //collect_set is used to collect values
    //Example: [25508.61,WrappedArray(L_EXTENDEDPRICE, O_TOTALPRICE)]

    //Value is removed (value is just useful for grouping of singletonType names)
    val attributeSets = groupedValueSingletonType
      .select("aggregatedAttributeSet")
      .distinct() //We remove duplicates because we are interested in inclusion dependencies (being sufficiently represented by one list)
      .as[Seq[String]] //We use a list (Seq is a list in scala) of all the strings
    //Example: List(O_ORDERKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY)



    //https://stackoverflow.com/questions/44436856/explode-array-data-into-rows-in-spark
    //Explode is used to distribute data into rows
    //Creates a new row for each element in the given array or map column
    //explode creates a row with each combination of the list elements as keys
    val combinationList = attributeSets
      .select(explode(attributeSets("aggregatedAttributeSet")).as("key"), col("aggregatedAttributeSet"))
      .as[(String, Seq[String])]
    /*
    //Example:
    (L_SUPPKEY,List(L_SUPPKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY))
    (S_SUPPKEY,List(L_SUPPKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY))
    (P_PARTKEY,List(L_SUPPKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY))
    (O_CUSTKEY,List(L_SUPPKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY))
    (C_CUSTKEY,List(L_SUPPKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY))
     */

    //with map we return a collection of elements after applying lambda function
    //https://stackoverflow.com/questions/12864505/how-can-i-idiomatically-remove-a-single-element-from-a-list-in-scala-and-close
    //filtering out first element in array
    val inclusionList = combinationList
      .map(row => (row._1, row._2.toList.filter(_ != row._1))) //row._1 is gettinng the first row (in python it would be row[0])
      .toDF("firstAttribute", "inclusionArray")
    //Example: [L_SUPPKEY,WrappedArray(S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY)]


    //We group all key values together
    val groupedValueInclusionList = inclusionList
      .groupBy(inclusionList("firstAttribute")).agg(collect_set(inclusionList("inclusionArray")).as("aggregatedAttributeSet"))
      .as[(String,Seq[Seq[String]])]
    //Example: (L_SUPPKEY,List(List(O_ORDERKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY), List(P_PARTKEY, S_SUPPKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, N_NATIONKEY, P_PARTKEY, S_SUPPKEY, P_SIZE, C_CUSTKEY, L_PARTKEY), List(S_NATIONKEY, N_REGIONKEY, P_PARTKEY, P_SIZE, R_REGIONKEY, L_PARTKEY, C_NATIONKEY, O_ORDERKEY, N_NATIONKEY, S_SUPPKEY, O_CUSTKEY, C_CUSTKEY, L_LINENUMBER), List(S_SUPPKEY, P_PARTKEY, P_SIZE, C_CUSTKEY, L_PARTKEY), List(S_SUPPKEY, P_PARTKEY, C_CUSTKEY), List(S_SUPPKEY, P_PARTKEY, P_SIZE, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, N_NATIONKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, C_CUSTKEY, L_PARTKEY), List(P_PARTKEY, S_SUPPKEY, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(O_ORDERKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(O_ORDERKEY, P_PARTKEY, S_SUPPKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, N_REGIONKEY, O_ORDERKEY, N_NATIONKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, R_REGIONKEY, C_CUSTKEY, L_LINENUMBER, L_PARTKEY), List(S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY), List(O_ORDERKEY, S_SUPPKEY, P_PARTKEY, C_CUSTKEY, L_PARTKEY), List(O_ORDERKEY, S_SUPPKEY, P_PARTKEY, C_CUSTKEY), List(P_PARTKEY, S_SUPPKEY, O_CUSTKEY, C_CUSTKEY), List(S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, N_NATIONKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(O_ORDERKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, C_CUSTKEY, L_PARTKEY), List(O_ORDERKEY, P_PARTKEY, S_SUPPKEY, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, O_ORDERKEY, N_NATIONKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, O_CUSTKEY, C_CUSTKEY, L_LINENUMBER, L_PARTKEY), List(S_SUPPKEY, P_PARTKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, N_NATIONKEY, P_PARTKEY, S_SUPPKEY, P_SIZE, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(O_ORDERKEY, P_PARTKEY, S_SUPPKEY, O_CUSTKEY, C_CUSTKEY), List(O_ORDERKEY, S_SUPPKEY, P_PARTKEY, O_CUSTKEY, C_CUSTKEY, L_PARTKEY), List(C_NATIONKEY, S_NATIONKEY, O_ORDERKEY, N_NATIONKEY, S_SUPPKEY, P_PARTKEY, P_SIZE, C_CUSTKEY, L_LINENUMBER, L_PARTKEY), List(P_PARTKEY, S_SUPPKEY, C_CUSTKEY)))

    //Find intersection af all key values of aggregated inclusion list
    //Reduce just applies a function to a collection (row._2 in this case)
    val partition = groupedValueInclusionList
      .map(row => (row._1,row._2.reduce(_.intersect(_)))) //row._1 is getting the first row (in python it would be row[0])
      .filter(row=> row._2.nonEmpty) //remove empty lists
      .toDF("firstAttribute", "partitionArray")

    val sortedPartition = partition
      .sort("firstAttribute") //sort alphabetically
      .as[(String,Seq[String])]


    sortedPartition
      .collect()
      .foreach(row => println(row._1 + " < " + row._2.reduce(_ + ", " + _)) )



  }
}