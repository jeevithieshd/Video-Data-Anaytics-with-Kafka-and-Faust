from kafka.admin import KafkaAdminClient, NewTopic

admin_client1 = KafkaAdminClient(
        bootstrap_servers = ['localhost:9092', 'localhost:9093'],
        client_id = 'Broker_Cluster_1'
)

# admin_client2 = KafkaAdminClient(
#         bootstrap_servers = ['localhost:9094','localhost:9095'],
#         client_id = 'Broker_Cluster_2'
# )

topic_list1 = []
topic_list1.append(NewTopic('image_frames', 2, 2))
topic_list1.append(NewTopic('corpus', 2, 2))
topic_list1.append(NewTopic('aggregation', 2, 2))

admin_client1.create_topics(topic_list1)

# topic_list2 = []
# topic_list2.append(NewTopic('corpus', 2, 2))
# topic_list2.append(NewTopic('aggregation', 2, 2))
# admin_client2.create_topics(topic_list2)
