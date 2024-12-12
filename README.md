# Projeto "Transactions Cards"

Desenvolvi o projeto **"Transactions Cards"**, focado no processamento de dados de cartões, usuários e transações.

O pipeline foi implementado com **Python (POO)** e integrado ao **Kafka** para extração em tempo real, armazenando dados brutos na camada **"rawzone"** do **MinIO Data Lake**.

Na etapa de transformação, utilizei **Pandas** para limpeza e processamento, movendo os dados para a camada **"silverzone"**. Na carga, realizei **modelagem dimensional** com **SQL** e armazenei os dados no **PostgreSQL**, com o ambiente gerenciado via **Docker**.

## Tecnologias Utilizadas:
- **Docker**
- **Python**
- **MinIO**
- **PostgreSQL**

## Estrutura do Projeto

```plaintext
src/
|── back-end/archive/
|                   ├── trasacntion_data.csv
│                   └── users_data.csv
|                   └── cards_data.csv             
├── kafka/
│       └── Producer.py
│       ├── Consumer.py       
│       ├── CsvWrite.py
│       ├── FileInDatalake.py
│       ├── Transformation.py
│       ├── Load.py
|       └── DataPipeline.py
├── minio_data/
│   ├── rawzone/transactions_cards/*                   
│   ├── silver/transactions_cards/*                    
│   └── gold/transactions_cards/*
│
├── postgres-db-volume                      
├── docker-compose.yaml            
├── Dockerfile.Producer
├── Requirements.txt
├── Main.py                    
