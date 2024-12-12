from kafka import KafkaConsumer
import time
from  src.kafka.CsvWrite import CsvWrite


class Consumer:

    def __init__(self, list_files):
         self.list_datas = list_files
         self.list_local_files=[]
    def Consumers_data(self):

        for consumer_data in self.list_datas:

            consumer = KafkaConsumer( 
            consumer_data,
            bootstrap_servers=['kafka-broker-1:9092'],  # Endereço do servidor Kafka
            group_id='data_transactions',     # Grupo de consumidores (opcional)
            auto_offset_reset='earliest',  # Início do consumo ('latest' ou 'earliest')
            enable_auto_commit=True,  # Confirmação automática de mensagens processadas
            value_deserializer=lambda x: x.decode('utf-8'),
            consumer_timeout_ms=10000)

            csv_write=CsvWrite(consumer_data)

            for message in consumer:
                csv_write.write_row(message.value)

            file_name_temp=csv_write.close()
            self.list_local_files.append(file_name_temp)
        return self.list_local_files
