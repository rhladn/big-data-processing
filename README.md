# distributed-big-data-systems
Spark Job.java: fetches Avro data from HDFS in the format Key->value1, value2... from different HDFS paths and
combines them and dumps data in the format of value1->key1, value2->key2 Avro format in HDFS and in Kafka
KafkaPublisher.java fetches data from HDFS to publish it to Kafka 
(this was implemented seperately to reduce parallelism while publishing to Kafka)
Employee.avsc input Avro format
EmployeeProfile.avsc output Avro format
PushToKafka.java Pushes data to Kafka as key, value pair (not implemented)
ApiUtils.java Uses GET request to make API calls to fetch data
SparkAvroUtils.java Sets Spark Avro format for input and output formats.
