from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Boolean, DECIMAL, CHAR, TIMESTAMP, Text
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from minio import Minio
from datetime import datetime
from io import BytesIO

Base = declarative_base()

# Definição da tabela usando SQLAlchemy ORM
class UsersData(Base):
    __tablename__ = 'users_data'
    id = Column(BigInteger, primary_key=True)
    current_age = Column(Integer)
    retirement_age = Column(Integer)
    birth_year = Column(Integer)
    birth_month = Column(Integer)
    gender = Column(String(10))
    address = Column(String(255))
    latitude = Column(String(255))
    longitude = Column(String(255))
    per_capita_income = Column(DECIMAL(10, 2))
    yearly_income = Column(DECIMAL(10, 2))
    total_debt = Column(DECIMAL(10, 2))
    credit_score = Column(DECIMAL(10, 2))
    num_credit_cards = Column(Integer)

class CardsData(Base):
    __tablename__ = 'cards_data'
    id = Column(BigInteger, primary_key=True)
    client_id = Column(BigInteger)
    card_brand = Column(String(20))
    card_type = Column(String(20))
    card_number = Column(BigInteger)
    expires = Column(String(30))
    cvv = Column(Integer)
    has_chip = Column(Boolean)
    num_cards_issued = Column(Integer)
    credit_limit = Column(DECIMAL(10, 2))
    acct_open_date = Column(String(30))
    year_pin_last_changed = Column(CHAR(4))
    credit_score = Column(DECIMAL(10, 2))
    card_on_dark_web = Column(Boolean)


class TransactionsData(Base):
    __tablename__ = 'transactions_data'
    id = Column(BigInteger, primary_key=True)
    date = Column(TIMESTAMP)
    client_id = Column(BigInteger)
    card_id = Column(Integer)
    amount = Column(DECIMAL(10, 2))
    use_chip = Column(String(30))
    merchant_id = Column(Integer)
    merchant_city = Column(String(255))
    merchant_state = Column(String(255))
    zip = Column(String(20))
    mcc = Column(Integer)
    errors = Column(Text)


class MccCodes(Base):
    __tablename__ = 'mcc_codes'
    id = Column(BigInteger, primary_key=True)
    description = Column(String(255))

class Load:

    def __init__(self, username, password, database,dir=None):

        self.username = username
        self.password = password
        self.database = database
        self.dir = dir
    
    def connect_DB(self):

        engine = create_engine(f'postgresql+psycopg2://{self.username}:{self.password}@kafka_pipeline-postgres-1:5432/{self.database}')
        return engine

    def connect_DataLake(self):

        minio_client = Minio(
            endpoint="minio:9000",
            access_key="AVcyU1dIcrfI88EqIa7p",
            secret_key="Wf2SYhz7K7LmsjSgFVW1Bbdtclakn98ujl66AcO4",
            secure=False
        )

        response = minio_client.get_object(
            bucket_name="silverzone",
            object_name=f'trasactions_cards/{self.dir}_{datetime.now().date()}.parquet'
        )

        return response

    def create_tables(self):

        engine = self.connect_DB()

        Base.metadata.create_all(engine)


    def insert_datas(self):

        engine = self.connect_DB()

        response = self.connect_DataLake()

        df = pd.read_parquet(BytesIO(response.data))

        if self.dir.startswith('transactions_data'):
            self.dir='transactions_data'

        df.to_sql(self.dir, con=engine, if_exists='append', index=False, method='multi')