import pandas as pd
from urllib.parse import quote_plus
import os
import dotenv
import Utils
from unidecode import unidecode


dotenv.load_dotenv('./.env')
execute = Utils.Utils()
# SEED_PRODUCTION_FIELDS

SEED_PRODUCTION_FIELDS_PATH_TO_SAVE = os.getenv('SEED_PRODUCTION_FIELDS_PATH')
SEED_PRODUCTION_FIELDS_FILE_URL = os.getenv('SEED_PRODUCTION_FIELDS_URL')
SEED_PRODUCTION_FIELDS_FILE_NAME = os.getenv('SEED_PRODUCTION_FIELDS_FILE_NAME')
SEED_PRODUCTION_FIELDS_TABLE_NAME = os.getenv('SEED_PRODUCTION_FIELDS_TABLE_NAME')

# db connection
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = quote_plus(os.getenv('DB_PASSWORD'))
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DATABASE = os.getenv('DATABASE')
DB_SCHEMA = os.getenv('DB_SCHEMA')

DB_CONNECTION_STRING = f'mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DATABASE}?driver=ODBC+Driver+17+for+SQL+Server'

ESPECIE_TO_FILTER = "Zea mays L."
SEASONS_TO_FILTER = ["2021/2021", "2021/2022", "2022/2022", "2022/2023","2023/2023", "2023/2024", "2024/2024", "2024/2025", "2025/2025", "2025/2026", "2026/2026", "2026/2027", "2027/2027"]
BRANDS_TO_FILTER = ["LPHT", "Corteva", "Bayer", "KWS", "LG Sementes", "Syngenta"]

engine = execute.connect_to_db(connection_string=DB_CONNECTION_STRING)

# SEED_PRODUCTION_FIELDS - download file
seed_production_fields_file = execute.download_file(file_url=SEED_PRODUCTION_FIELDS_FILE_URL, path_to_save=SEED_PRODUCTION_FIELDS_PATH_TO_SAVE, file_name=SEED_PRODUCTION_FIELDS_FILE_NAME)
if seed_production_fields_file:
    df = execute.read_file(path_to_save=SEED_PRODUCTION_FIELDS_PATH_TO_SAVE)
    df = df.rename(columns={"Safra": "season","Especie": "species","Categoria": "category","Cultivar": "hybrid","Municipio": "municipio","UF": "uf","Status": "status", "Data do Plantio": "dataplantio", "Data de Colheita": "datacolheita", "Producao bruta": "producaobruta", "Producao estimada": "producaoestimada"})
    df['correct_planting_date'] = pd.to_datetime(df['dataplantio'], errors='coerce', format='%d/%m/%Y', dayfirst=True).dt.strftime('%d/%m/%Y')
    filtered_df = df[df['species'] == ESPECIE_TO_FILTER]
    filtered_df = filtered_df[filtered_df['season'].isin(SEASONS_TO_FILTER)]

    hybrids_by_brand_df = pd.read_sql_table('HYBRIDS_BY_BRAND', con=engine, schema=DB_SCHEMA)

    merged_df = pd.merge(filtered_df, hybrids_by_brand_df, on='hybrid', how='left')
    merged_df = merged_df[merged_df['cia'].isin(BRANDS_TO_FILTER)]
    merged_df['hybrid'] = merged_df.apply(lambda row: row['hybrid'] if (row['apelido'] == None or row['apelido'] == "")   else row['apelido'], axis=1)
    merged_df.loc[merged_df['apelido'].notnull(), 'hybrid'].apply(lambda row: row)

    merged_df["city_state"] = merged_df["municipio"] + "-" + merged_df["uf"]
    merged_df['city_state'] = merged_df['city_state'].astype(str).apply(unidecode)

    geo_lat_long_df = pd.read_sql_table('GEO_LAT_LONG', con=engine, schema=DB_SCHEMA)
    geo_lat_long_df = geo_lat_long_df.drop(columns=['city', 'state', 'country'])

    new_merged_df = pd.merge(merged_df, geo_lat_long_df, on='city_state', how='left')
    new_merged_df = new_merged_df.drop(columns=['datacolheita', 'apelido'])
    new_merged_df = new_merged_df.rename(columns={"municipio": "city", "uf": "state", "uf": "state", "marca": "brand", "Area": "area", "dataplantio": "harvest_date", "producaobruta": "gross_production", "producaoestimada": "estimated_production"})

    deleted_table = execute.delete_data_from_table(engine=engine, table_schema=DB_SCHEMA, table_name='SEED_PRODUCTION_FIELDS' )

    execute.send_data_to_sql(dataframe=new_merged_df, table_name=SEED_PRODUCTION_FIELDS_TABLE_NAME, connection_string=DB_CONNECTION_STRING, schema=DB_SCHEMA)

    new_merged_df.to_excel(f"C:/Users/7616594/Downloads/SEED_PRODUCTION_FIELDS_{execute.time_to_save()}.xlsx", index=False)
