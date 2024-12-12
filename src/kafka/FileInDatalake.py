from minio import Minio
from datetime import datetime
import time
import os
import re
from pathlib import Path

class FileInDatalake:

    def __init__(self,bucket_name,file_path,Local_minio):
        self.bucket_name=bucket_name
        self.file_path=file_path
        self.local_minio = Local_minio

    def connect_datalake(self):
    
        minio_client = Minio(
        'minio:9000',
        access_key="AVcyU1dIcrfI88EqIa7p",  # Chave de acesso
        secret_key="Wf2SYhz7K7LmsjSgFVW1Bbdtclakn98ujl66AcO4",  # Chave secreta
        secure=False  # True se usar HTTPS)
        )
        file_name = os.path.basename(self.local_minio)

        match = re.match(r'^(transactions_data_\d+)', Path(file_name).stem)
        if match:
            file_name = match.group(1)

        if file_name.startswith('users_data') and self.bucket_name =='rawzone':
            self.local_minio=f'trasactions_cards/users_data_{datetime.now().date()}.csv'

        if file_name.startswith('cards_data') and self.bucket_name =='rawzone':
            self.local_minio=f'trasactions_cards/cards_data_{datetime.now().date()}.csv'

        if file_name.startswith('transactions_data') and self.bucket_name =='rawzone':
            self.local_minio=f'trasactions_cards/{file_name}_{datetime.now().date()}.csv'

        
        if file_name.startswith('users_data') and self.bucket_name =='silverzone':
            self.local_minio=f'trasactions_cards/users_data_{datetime.now().date()}.parquet'

        if file_name.startswith('cards_data') and self.bucket_name =='silverzone':
            self.local_minio=f'trasactions_cards/cards_data_{datetime.now().date()}.parquet'

        if file_name.startswith('transactions_data') and self.bucket_name =='silverzone':
            self.local_minio=f'trasactions_cards/{file_name}_{datetime.now().date()}.parquet'


        minio_client.fput_object(
        bucket_name=self.bucket_name,
        object_name=self.local_minio,
        file_path=self.file_path)