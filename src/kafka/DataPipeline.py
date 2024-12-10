import time
import json
from pathlib import Path
from src.kafka.FileInDatalake import FileInDatalake
import os
import re
class DataPipeline():

    def __init__(self, Consumer, Transformation, Load):
        self.Consumer = Consumer
        self.Transformation = Transformation
        self.Load = Load


    def run(self):


        time.sleep(70)

        list_names_datas_cosumedes=self.Consumer.Consumers_data()

        for name in list_names_datas_cosumedes:
            datalake = FileInDatalake('rawzone',file_path=name,Local_minio=name)
            datalake.connect_datalake()
            
            file_name = os.path.basename(name)

            self.Transformation.name_file=file_name
            dict_files=self.Transformation.run()
           
            match = re.search(r'(users_data|cards_data|transactions_data)', file_name)

            if match:
                key_dict = match.group(0)  

            print("TESTE")
            print(dict_files[key_dict])
            datalake_silver = FileInDatalake('silverzone',file_path=dict_files[key_dict],Local_minio=name)
            datalake_silver.connect_datalake()
            
            
