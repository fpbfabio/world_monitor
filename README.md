# World Monitor

A system for monitoring disasters in the world in real time.

Following the Kappa architecture, the system should consist of a python crawler, a java program for importing the crawler data to Apache Kafka and a java program to consume data from Kafka, process it with Apache Flink and output the result to some NoSQL database.
