# Fraud detector

##Build project
`assembly`

##Start Apache Spark application
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --class com.github.aborgatin.stream.finalproject.spark.Application spark-streaming-fraud-detector-0.1.jar`

##Start Ignite node
`java -cp spark-fraud.jar com.github.aborgatin.stream.finalproject.ignite.IgniteNodeStart`
