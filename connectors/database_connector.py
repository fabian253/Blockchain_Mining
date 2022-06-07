from mysql.connector.locales.eng import client_error
import mysql.connector


class MySqlDatabaseConnector:

    def __init__(self) -> None:
        self.cnx = None

    def __del__(self) -> None:
        try:
            self.cnx.close()
        except Exception:
            pass

    def connect(self, host, user, password):
        self.cnx = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )

    def __execute_command(self, command):
        cursor = self.cnx.cursor()
        cursor.execute(command)
        result = cursor.fetchall()
        cursor.close()
        return result

    def create_database(self, name):
        self.__execute_command(f"CREATE DATABASE {name}")

    def use_database(self, name):
        self.__execute_command(f"USE {name}")

    def insert_value(self, table_name, value_dict):
        columns = []
        values = []
        for key, value in value_dict.items():
            columns.append(key)
            value = str(value).replace("'", "''")
            values.append(f"'{value}'")

        command = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
        command = command.replace("\,", ",")

        self.__execute_command(command)

        self.cnx.commit()

    def select_value(self, query):
        test = self.__execute_command(query)
        return test
