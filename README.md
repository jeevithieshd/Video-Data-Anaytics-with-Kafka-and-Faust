# Video-Data-Anaytics-with-Kafka-and-Faust

1. Run the Zookeeper with zookeeper_cluster_1.properties in the Zookeepers folder
2. Run the Servers with server_cluster_1_1.properties and server_cluster_1_2.properties in the Servers folder
3. Place the Video files to be analysed in the Data/Data1 and Data/Data2 folders.
4. Run the two producer python files in the Producer folder.
5. Run the Faust Consumers with the command faust -A \<consumer-file\> worker.
