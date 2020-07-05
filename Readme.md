This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit. In order to run this, you need to
- install sbt
- add scala plugin to your IDE of choice (in the following we describe this for Intellij)
- check out the project and open it in your IDE
- Intellij should automatically recognize this as an sbt project (initialization, for example indexing files might take a while)
- If Intellij does not automatically download the depencencies, you can either open the console and type **sbt compile** or open the sbt tab (right edge of the screen) and click "Reload all sbt projects"
- Now you should be able to execute the code in Intellij
- For default running configuration put the TPCH data into the folder named "TPCH" and place it into the root directory of the project
- Example for customized running configuration: " --path TPCH --cores 8 "
- Run **sbt clean assembly** in your console to build a Fat-jar for deployment 

**Steps for discovering Inclusion Dependencies** (see "Scaling Out the Discovery of Inclusion Dependencies" by Sebastian Kruse, Thorsten Papenbrock, Felix Naumann)
https://www.researchgate.net/publication/296160215_Scaling_Out_the_Discovery_of_Inclusion_Dependencies
Input Tuples --> Cells
	- Splitting each record with (value, (Singleton type))
Cells --> Cache-based Preaggregation (optional when repeatedly occuring cells with values)
	- Should reduce network load
	- Groups by the value, and the singleton type
 Cache-based Preaggregation --> Global partitioning
	- Reordering the cells among the workers of the cluster through hashing
	- We need an appropriate function to map each different value to unique cell
	- Therefore: cells with the same value are on the same worker
Global partitioning --> Attribute Sets
	- Grouping all cells by their values
	- Aggregating attribute sets using union operator
Attribute Sets -->  Inclusion Lists
	- Set with n attributes = n inclusion lists (all possible combinations)
Inclusion List --> Partition
	- Group by the first attribute
Partition --> Aggregate
	- Intersection with preaggregation
	- Ends with attributes with empty sets; no (n,0)
Aggregate --> INDs
  - Disassembling into INDs
