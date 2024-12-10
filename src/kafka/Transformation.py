import pandas as pd
import re
from datetime import datetime
import tempfile
import pyarrow
from minio import Minio
from io import BytesIO

class Transformations:

    def __init__(self, name_file=None):
        self.name_file=name_file
        self.dict_files ={}

    
    def read_file(self,dir):
        
        minio_client = Minio(
            endpoint="minio:9000",
            access_key="AVcyU1dIcrfI88EqIa7p",
            secret_key="Wf2SYhz7K7LmsjSgFVW1Bbdtclakn98ujl66AcO4",
            secure=False
        )

        response = minio_client.get_object(
                    bucket_name="rawzone",
                    object_name=f'trasactions_cards/{dir}{datetime.now().date()}.csv')

        return response


    def run(self):
       

        if self.name_file and self.name_file.startswith('users_data'):

            response=self.read_file('users_data_')

            data = pd.read_csv(BytesIO(response.data),header=None)

            data.drop(index=0, inplace=True)

            cleaned_data = [str(row).strip('"').strip('\\n') for row in data[0]] 

            split_data = [row.split(',') for row in cleaned_data]

            colunms = ['id','current_age','retirement_age','birth_year','birth_month','gender','address','latitude',
                    'longitude','per_capita_income','yearly_income','total_debt','credit_score','num_credit_cards']

            df = pd.DataFrame(split_data, columns=colunms)

            monetary_columns = ['per_capita_income', 'yearly_income', 'total_debt']
            for col in monetary_columns:
                df[col] = df[col].replace({'\$': ''}, regex=True).astype(float)

            for col in ['birth_year', 'birth_month', 'num_credit_cards','current_age','id','credit_score','num_credit_cards','retirement_age']:
                df[col] = pd.to_numeric(df[col], errors='coerce')


            with tempfile.NamedTemporaryFile(suffix='.parquet',delete=False) as tem_file:
                temp_path = tem_file.name
                df.to_parquet(temp_path,engine='pyarrow', index=False)
                self.dict_files['users_data']=temp_path
            
        if self.name_file and self.name_file.startswith('cards_data'):

            response=self.read_file('cards_data_')

            data = pd.read_csv(BytesIO(response.data),header=None)

            

            data.drop(index=0, inplace=True)

            cleaned_data = [str(row).strip('"').strip('\\n') for row in data[0]] 

            split_data = [row.split(',') for row in cleaned_data]

            colunms = ['id','client_id','card_brand','card_type','card_number','expires','cvv','has_chip','num_cards_issued','credit_limit','acct_open_date','year_pin_last_changed','card_on_dark_web']
                
            df = pd.DataFrame(split_data, columns=colunms)

            df['credit_limit'] =df['credit_limit'].replace({'\$': ''}, regex=True).astype(float)

            

            for col in ['id', 'client_id','year_pin_last_changed','num_cards_issued']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            with tempfile.NamedTemporaryFile(suffix='.parquet',delete=False) as tem_file:
                temp_path = tem_file.name
                df.to_parquet(temp_path,engine='pyarrow', index=False)
                self.dict_files['cards_data']=temp_path


        if self.name_file and self.name_file.startswith('transactions_data'):

            response=self.read_file('transactions_data_')

            data = pd.read_csv(BytesIO(response.data),header=None)

            print('Entrou em  transactions_data ')
            
            print(data)

            data.drop(index=0, inplace=True)

            cleaned_data = [str(row).strip('"').strip('\\n') for row in data[0]] 

            split_data = [row.split(',') for row in cleaned_data]

            colunms = ['id','date','client_id','card_id','amount','use_chip','merchant_id','merchant_city','merchant_state','zip','mcc','errors']
                
            df = pd.DataFrame(split_data, columns=colunms)

            df['amount'] =df['amount'].replace({'\$': ''}, regex=True).astype(float)

            print(df)

            for col in ['id', 'client_id','card_id','errors']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            with tempfile.NamedTemporaryFile(suffix='.parquet',delete=False) as tem_file:
                temp_path = tem_file.name
                df.to_parquet(temp_path,engine='pyarrow', index=False)
                self.dict_files['transactions_data']=temp_path   

            
        return self.dict_files