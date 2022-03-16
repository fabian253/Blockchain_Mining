from dataclasses import replace
import mysql.connector


class NftDatabaseConnector:

    def __init__(self) -> None:
        self.cnx = None

    def connect(self, host, user, password):
        self.cnx = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )

    def disconnect(self):
        self.cnx.close()

    def __execute_command(self, command):
        cursor = self.cnx.cursor()
        cursor.execute(command)
        cursor.close()

    def create_database(self, name):
        self.__execute_command(f"CREATE DATABASE {name}")

    def use_database(self, name):
        self.__execute_command(f"USE {name}")

    def insert_nft(self, table_name, nft_dict: dict):
        columns = []
        values = []
        for key, value in nft_dict.items():
            columns.append(key)
            value = str(value).replace("'", "''")
            values.append(f"'{value}'")

        command = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
        command = command.replace("\,", ",")

        self.__execute_command(command)

        self.cnx.commit()
