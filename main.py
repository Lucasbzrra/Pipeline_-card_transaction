from src.kafka.DataPipeline import DataPipeline
from src.kafka.Consumer import Consumer
from src.kafka.Producer import Producer
from src.kafka.Transformation import Transformations
import time
from pathlib import Path

if __name__ =='__main__':



    time.sleep(50)

    directory = Path('src/back-end/archive/')

    csv_files = list(directory.glob('*.csv'))

    

    for archive in csv_files:

        topic_name = archive.name.replace('.csv','')
        producer=Producer(archive,topic_name)
        producer.read_archive_for_kafka()
        


    consumer = Consumer()
    transformation = Transformations()
    pipeline = DataPipeline(Consumer=consumer,Transformation=transformation,Load=None)
    pipeline.run()