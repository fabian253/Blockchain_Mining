from matplotlib.pyplot import table
from connectors.database_connector import MySqlDatabaseConnector
import pandas as pd


class BlockchainDatabaseConnector:

    def __init__(self, host, user, password, database_name) -> None:
        self.db_cnx = MySqlDatabaseConnector()
        self.db_cnx.connect(host, user, password)
        self.db_cnx.use_database(database_name)

    # query distinct values from column in table
    def query_distinct_values(self, table_name, column_name) -> pd.DataFrame:
        distinct_value_data = self.db_cnx.select_value(
            f"select distinct {column_name} from {table_name};")

        distinct_value_data = [value[0] for value in distinct_value_data]

        distinct_value_df = pd.DataFrame(
            distinct_value_data, columns=[column_name])

        return distinct_value_df

    # query distinct wallets by "owner_of" column in table
    # contract_address is optional
    def query_distinct_wallets(self, table_name, contract_address=None) -> pd.DataFrame:
        if contract_address == None:
            wallets = self.db_cnx.select_value(
                f"select distinct owner_of from {table_name};")
        else:
            wallets = self.db_cnx.select_value(
                f"select distinct owner_of from {table_name} where token_address = '{contract_address}';")

        wallet_df = pd.DataFrame(wallets, columns=["wallet_address"])

        return wallet_df

    # query nft data (all columns)
    # contract_address is optional
    def query_nft_data(self, table_name, contract_address=None) -> pd.DataFrame:
        if contract_address == None:
            nft_data = self.db_cnx.select_value(f"select * from {table_name};")
        else:
            nft_data = self.db_cnx.select_value(
                f"select * from {table_name} where token_address = '{contract_address}';")

        column_names = [
            "token_address",
            "token_id",
            "contract_type",
            "owner_of",
            "block_number",
            "block_number_minted",
            "token_uri",
            "metadata",
            "synced_at",
            "amount",
            "name",
            "symbol",
            "token_hash",
            "last_token_uri_sync",
            "last_metadata_sync"
        ]

        nft_data_df = pd.DataFrame(nft_data, columns=column_names)

        return nft_data_df

    # query ft balance data (all columns)
    # wallet_address is optional
    def query_ft_balance_data(self, table_name, wallet_address=None) -> pd.DataFrame:
        if wallet_address == None:
            ft_balance_data = self.db_cnx.select_value(
                f"select * from {table_name};")
        else:
            ft_balance_data = self.db_cnx.select_value(
                f"select * from {table_name} where owner_of = '{wallet_address}';")

        column_names = [
            "owner_of",
            "token_address",
            "name",
            "symbol",
            "logo",
            "thumbnail",
            "decimals",
            "balance"
        ]

        ft_balance_data_df = pd.DataFrame(
            ft_balance_data, columns=column_names)

        return ft_balance_data_df

    # query nft balance data (all columns)
    # wallet_address is optional
    def query_nft_balance_data(self, table_name, wallet_address=None) -> pd.DataFrame:
        if wallet_address == None:
            nft_balance_data = self.db_cnx.select_value(
                f"select * from {table_name};")
        else:
            nft_balance_data = self.db_cnx.select_value(
                f"select * from {table_name} where owner_of = '{wallet_address}';")

        column_names = [
            "token_address",
            "token_id",
            "contract_type",
            "owner_of",
            "block_number",
            "block_number_minted",
            "token_uri",
            "metadata",
            "synced_at",
            "amount",
            "name",
            "symbol",
            "token_hash",
            "last_token_uri_sync",
            "last_metadata_sync"
        ]

        nft_balance_data_df = pd.DataFrame(
            nft_balance_data, columns=column_names)

        return nft_balance_data_df

    # query nft transfer data (all columns)
    # wallet_address is optional
    def query_nft_transfer_data(self, table_name, wallet_address=None) -> pd.DataFrame:
        if wallet_address == None:
            nft_transfer_data = self.db_cnx.select_value(
                f"select * from {table_name};")
        else:
            nft_transfer_data = self.db_cnx.select_value(
                f"select * from {table_name} where from_address = '{wallet_address}' or to_address = '{wallet_address}';")

        column_names = [
            "block_number",
            "block_timestamp",
            "block_hash",
            "transaction_hash",
            "transaction_index",
            "log_index",
            "value",
            "contract_type",
            "transaction_type",
            "token_address",
            "token_id",
            "from_address",
            "to_address",
            "amount",
            "verified",
            "operator"
        ]

        nft_transfer_data_df = pd.DataFrame(
            nft_transfer_data, columns=column_names)

        return nft_transfer_data_df

    # insert nft data
    def insert_nft_data(self, table_name, nft_data_list: list):
        for nft_data in nft_data_list:
            self.db_cnx.insert_value(table_name, nft_data)

    # insert ft balance data
    def insert_ft_balance_data(self, table_name, ft_balance_data_list: list):
        for ft_balance_data in ft_balance_data_list:
            self.db_cnx.insert_value(table_name, ft_balance_data)

    # insert nft balance data
    def insert_nft_balance_data(self, table_name, nft_balance_data_list: list):
        for nft_balance_data in nft_balance_data_list:
            self.db_cnx.insert_value(table_name, nft_balance_data)

    # insert ft transfer data
    def insert_ft_transfer_data(self, table_name, ft_transfer_data_list: list):
        for ft_transfer_data in ft_transfer_data_list:
            self.db_cnx.insert_value(table_name, ft_transfer_data)

    # insert nft transfer data
    def insert_nft_transfer_data(self, table_name, nft_transfer_data_list: list):
        for nft_transfer_data in nft_transfer_data_list:
            self.db_cnx.insert_value(table_name, nft_transfer_data)
