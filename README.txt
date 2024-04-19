This project is about a real time streaming simulation. 

We are using Kafka, MongoDB and PowerBI for this project.

Workflow :

- Kafka server is setup in a Oracle Virtualbox Ubuntu Server.
- Kafka and Zookeeper servers are started
- Producer code is run in the Kafka server. It gets the data from Yahoo Finance (Apple) Stock price data and pushes the data to Kafka Cluster.
- Consumer code is run in the local server. It receives the data from Kafka server pushes the data to MongoDB.
- Another script in the local system connects MongoDB with PowerBI and pushes the data to PowerBI. The schema has to be pre defined in PowerBI.
- An analytics script is run in the local system which is getting the data from MongoDB and doing analysis and visualization as well.