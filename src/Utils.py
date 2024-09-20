from datetime import datetime
import requests
import dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()


class Utils:
    def __init__(self):
        pass

    def download_file(self, file_url, path_to_save, file_name):
        """
        Downloads a file from the given URL and saves it to the specified path.

        Args:
            file_url (str): The URL of the file to download.
            path_to_save (str): The path where the file should be saved.
            file_name (str): The name of the file.

        Returns:
            bool: True if the file was downloaded successfully, False otherwise.
        """
        response = requests.get(file_url)
        if response.status_code == 200:
            with open(path_to_save, 'wb') as file:
                file.write(response.content)
            print(f'\033[92mFile {file_name} was downloaded successfully to {path_to_save}.\033[00m')
            return True
        else:
            print(f'\033[91mFailed to download file {file_name}. HTML Status code: {response.status_code}.\033[00m')
            return False


    def read_file(self, path_to_save):
        """
        Reads a CSV file from the given path and returns a pandas DataFrame.

        Args:
            path_to_save (str): The path to the CSV file.

        Returns:
            pandas.DataFrame: The DataFrame containing the data from the CSV file.
        """
        df = pd.read_csv(path_to_save, sep=';', encoding='utf-8', low_memory=False)
        return df


    def connect_to_db(self, connection_string):
        engine = create_engine(connection_string)
        return engine


    def send_data_to_sql(self, dataframe, table_name, connection_string, schema):
        """
        Sends the data from a DataFrame to a SQL table.

        Args:
            table_name (str): The name of the table to send the data to.
            connection_string (str): The connection string for the SQL database.

        Returns:
            bool: True if the data is successfully sent.
        """ 
        engine = self.connect_to_db(connection_string)
        execution = dataframe.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
        return execution


    def delete_data_from_table(self, engine, table_schema, table_name):
        Session = sessionmaker(bind=engine)
        session = Session()

        # Defina a estrutura da tabela (substitua "table_name" pelo nome da sua tabela)
        metadata = MetaData()
        try:
            table = Table(table_name, metadata, autoload_with=engine, schema=table_schema)

            session.query(table).delete()
            session.commit()
            session.close()
        except Exception as e:
            print(f'\033[91mFailed to delete data from table {table_name}. Error: {e}.\033[00m')
            session.close()
            
            
    def time_to_save(self):
        now = datetime.now().strftime('%d%m%Y_%H%M%S')
        return now
        