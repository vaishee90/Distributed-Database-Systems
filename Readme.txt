Mapper:
======
- The EquiJoinMapper class contains a 'map' method which takes each line from the input file in the HDFS location and sets the the key value pair respectively. 
- The key is set to be the second value in the line as specified in the assignment document (which is the 'joinKey' variable in the code) and the value is set to be the line read from the input file. - The map function also takes note of the table names provided in the input file in a Hashmap called 'tableNames'.

Reducer:
=======
- The EquiJoinReducer class contains a 'reduce' method takes the key set in the mapper phase and the values which were mapped to that particular key as arguments in order to output the equijoin results. 
- The method first updates the 'tableNames' Hashmap if the values belong to a particular table and also puts the values into a recordList. Then the 'tableNames' Hashmap is checked for a zero count for any of the table. If there is a 0, that value(s) for that key is not written to the output file since values from both the tables are required to perform the equijoin.
- Finally, if the 'tableNames' Hashmap does not contain any zeroes, the values for the given key are concatenated. The final output from the Reducer is then written to the output file in the HDFS location.

Driver:
======
- The driver is implemented in the main method under the equijoin class. It sets all the configuration and job details such as the Mapper, Reducer, MapOutputKey, MapOutputValue, OutputKey and OutputValue classes. It also specifies the input and output paths for the map-reduce program. It kickstarts the job and waits till completion.