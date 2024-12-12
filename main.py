from src.kafka.DataPipeline import DataPipeline
from src.kafka.Consumer import Consumer
from src.kafka.Producer import Producer
from src.kafka.Transformation import Transformations
from src.kafka.Load import Load
import time
from pathlib import Path

if __name__ =='__main__':


    time.sleep(70)

    directory = Path('src/back-end/archive/')

    list_csv_files = list(directory.glob('*.csv'))

    list_files=[]

    for archive in list_csv_files:

        topic_name = archive.name.replace('.csv','')
        list_files.append(topic_name)
        producer=Producer(archive,topic_name)
        producer.read_archive_for_kafka()
        

    consumer = Consumer(list_files=list_files)

    transformation = Transformations()

    load = Load(username='project',password='project',database='Dw_project')

    pipeline = DataPipeline(Consumer=consumer,Transformation=transformation,Load=load)

    pipeline.run()