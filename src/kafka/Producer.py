from kafka import KafkaProducer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic
import time
import json
from pathlib import Path


class Producer: 

    def __init__(self,file_path,topic_name):
        self.topic_name = topic_name
        self.file_path=file_path

    def create_topic(self,admin_client, topic_name):
   
        topics = admin_client.list_topics()
   
        if topic_name not in topics:
       
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics([topic])
       
    def read_archive_for_kafka(self):

        admin_client = KafkaAdminClient(
        bootstrap_servers=['kafka-broker-1:9092']
        )
        
        self.create_topic(admin_client ,self.topic_name)

        producer = KafkaProducer(
            bootstrap_servers=['kafka-broker-1:9092'],
            request_timeout_ms=10000,  # Adjust timeout as needed
            retries=5,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        filepath_=self.file_path
        with open(filepath_) as file:

            for line in file:
                producer.send(f'{self.topic_name}',value=line)


