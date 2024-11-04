import os
import dotenv
import Utils
import csv
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()
execute = Utils.Utils()

# db connection
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DATABASE = os.getenv('DATABASE')
DB_SCHEMA = os.getenv('DB_SCHEMA')

DB_CONNECTION_STRING = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'
TABLE_NAME = 'HYBRIDS_BY_BRAND'

# Crie a engine (substitua "sqlite:///example.db" pela string de conexão do seu banco de dados)
engine = create_engine(DB_CONNECTION_STRING)

# Crie uma sessão
Session = sessionmaker(bind=engine)
session = Session()

# Defina a estrutura da tabela (substitua "table_name" pelo nome da sua tabela)
metadata = MetaData()
table = Table(TABLE_NAME, metadata, autoload_with=engine, schema=DB_SCHEMA)

table.delete()

# Abre o arquivo .csv
with open("C:/Users/7616594/Downloads/HYBRIDS_BY_BRAND_v4.csv", 'r') as f:
    csv_reader = csv.reader(f, delimiter=';', skipinitialspace=True, dialect='excel')
    headers = ["cia", "marca", "hybrid", "apelido"]
    for row in csv_reader:
        # Crie um novo objeto da tabela e adicione-o à sessão
        obj = {key: value for key, value in zip(headers, row)}
        insert_stmt = table.insert().values(**obj)
        session.execute(insert_stmt)

# Faça commit da sessão para inserir os dados no banco de dados
session.commit()

# Feche a sessão
session.close()
