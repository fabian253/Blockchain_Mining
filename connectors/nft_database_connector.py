from connectors.database_connector import MySqlDatabaseConnector
import pandas as pd


class NftDatabaseConnector:

    def __init__(self, host, user, password, database_name) -> None:
        self.db_cnx = MySqlDatabaseConnector()
        self.db_cnx.connect(host, user, password)
        self.db_cnx.use_database(database_name)

    def __del__(self) -> None:
        self.db_cnx.disconnect()

    def query_current_nft_owners(self, contract_address=None) -> list:
        if contract_address == None:
            nft_owners = self.db_cnx.select_value(
                "select owner_of from nft;")
        else:
            nft_owners = self.db_cnx.select_value(
                f"select owner_of from nft where token_address = '{contract_address}';")

        nft_owners_df = pd.DataFrame(nft_owners, columns=["nft_owners"])

        return nft_owners_df["nft_owners"].tolist()

    def query_all_nft_data(self) -> pd.DataFrame:
        data = self.db_cnx.select_value("select * from nft;")

        column_names = [
            "token_address",
            "token_id",
            "block_number_minted",
            "owner_of",
            "block_number",
            "token_hash",
            "amount",
            "contract_type",
            "name",
            "symbol",
            "token_uri",
            "metadata",
            "synced_at"
        ]

        data_df = pd.DataFrame(data, columns=column_names)

        return data_df

    def query_unique_nft_owners(self, contract_address=None) -> list:
        nft_owners = self.query_current_nft_owners(contract_address)

        nft_owners_df = pd.DataFrame(nft_owners, columns=["nft_owners"])

        return nft_owners_df["nft_owners"].unique().tolist()

    def query_nft_transfers(self) -> pd.DataFrame:
        nft_transfers = self.db_cnx.select_value(
            "select from_address, to_address from transfers;")

        nft_transfers_df = pd.DataFrame(
            nft_transfers, columns=["from_address", "to_address"])

        return nft_transfers_df

    def insert_nfts(self, nft_list, table_name):
        for nft in nft_list:
            self.db_cnx.insert_value(table_name, nft)

    def insert_nft_transfers(self, transfer_list, table_name):
        for transfer in transfer_list:
            self.db_cnx.insert_value(table_name, transfer)

    def insert_erc20_transfers(self, erc20_transfer_list, table_name):
        for transfer in erc20_transfer_list:
            self.db_cnx.insert_value(table_name, transfer)
