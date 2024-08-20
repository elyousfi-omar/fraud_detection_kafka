from KafkaProducer import KafkaProducer
import pandas as pd
from utils import *
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def main():

    TABLE_NAME = "transactions_data"
    DB_NAME = "client_data"

    DB_USER = os.getenv("DB_USER")
    DB_PW = os.getenv("DB_PW")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}")
    data_query = f"SELECT * from {DB_NAME}.{TABLE_NAME}"
    data = pd.read_sql(data_query, engine)

    data = data[~data["settlement_amount"].isna()]

    row_1 = data.sample(1)
    
    producer = KafkaProducer("KafkaProducer\client.properties")

    producer.produce("transactions", row_1.to_dict(orient="records")[0], "transaction")

    return 200

if __name__ == "__main__":
    main()