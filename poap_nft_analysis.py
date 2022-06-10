from ipaddress import collapse_addresses
from connectors.blockchain_database_connector import BlockchainDatabaseConnector
import pandas as pd
import numpy as np
import re
import json
import config


# get common pfp and poap wallets
def get_common_pfp_poap_wallets(pfp_data_df: pd.DataFrame, poap_data_df: pd.DataFrame):

    pfp_wallets = pfp_data_df["owner_of"].unique()

    poap_wallets = poap_data_df["owner_of"].unique()

    common_wallets = [
        wallet for wallet in pfp_wallets if wallet in poap_wallets]

    return common_wallets


# insert poap name from metadata
def insert_poap_name_from_metadata(poap_data_df: pd.DataFrame) -> pd.DataFrame:
    poap_name_column = []

    for idx, row in poap_data_df.iterrows():
        metadata = str(row["metadata"])
        metadata = metadata.replace("\n", " ").replace(
            "\r", " ").replace("  ", " ")
        # remove qotation marks in dict values
        metadata = re.sub('[^{:,\[](["])[^}:,\]]', '', metadata)
        if metadata != "None":
            try:
                metadata_dict = json.loads(metadata)
                poap_name_column.append(metadata_dict["name"])
            except Exception as e:
                poap_name_column.append("None")
        else:
            poap_name_column.append("None")

    poap_data_df["poap_name"] = poap_name_column

    return poap_data_df


# get poap frequency (number of attendence for each poap) (save to file)
def calc_poap_frequency(poap_data_df: pd.DataFrame, poap_frequency_file_name):
    poap_frequency_df = poap_data_df.groupby(
        ["poap_name"], as_index=False).size()

    poap_frequency_df = poap_frequency_df.rename(columns={"size": "frequency"})

    poap_frequency_df = poap_frequency_df[poap_frequency_df["poap_name"] != "None"]

    poap_frequency_df = poap_frequency_df.sort_values(
        by=["frequency"], ascending=False)

    poap_frequency_df = poap_frequency_df.reset_index(drop=True)

    poap_frequency_df.to_csv(poap_frequency_file_name)


# calc nft frequency of poap owners (save to file)
def calc_nft_frequency_of_poap_wallets(nft_balance_df: pd.DataFrame, wallet_address_list, nft_frequency_file_name):
    nft_wallet_frequency_df = nft_balance_df.groupby(
        ["owner_of", "token_address", "name"], as_index=False).size()

    nft_wallet_frequency_df = nft_wallet_frequency_df[nft_wallet_frequency_df["owner_of"].isin(
        wallet_address_list)]

    nft_frequency_df = nft_wallet_frequency_df.groupby(
        ["token_address", "name"], as_index=False).size()

    nft_frequency_df = nft_frequency_df.rename(columns={"size": "frequency"})

    nft_frequency_df = nft_frequency_df.sort_values(
        by=["frequency"], ascending=False)

    nft_frequency_df = nft_frequency_df.reset_index(drop=True)

    nft_frequency_df.to_csv(nft_frequency_file_name)


# calc nft frequency by wallets (save to file)
def calc_nft_count_by_wallet(nft_balance_df: pd.DataFrame, wallet_address_list, nft_count_file_name):
    nft_frequency_df = nft_balance_df.groupby(
        ["owner_of"], as_index=False).size()

    nft_frequency_df = nft_frequency_df[nft_frequency_df["owner_of"].isin(
        wallet_address_list)]

    nft_frequency_df = nft_frequency_df.rename(
        columns={"size": "number of tokens"})

    nft_frequency_df.to_csv(nft_count_file_name)

    average_number_of_tokens = nft_frequency_df["number of tokens"].mean()

    return average_number_of_tokens


# calc attendance frequency (save to file)
def calc_attendance_frequency(poap_data_df: pd.DataFrame, poap_attendance_frequency_file_name):
    attendance_freq_df = poap_data_df.groupby(
        ["owner_of"], as_index=False).size()

    attendance_freq_df = attendance_freq_df.groupby(
        ["size"], as_index=False).count()

    attendance_freq_df = attendance_freq_df.rename(
        columns={"size": "number of attendance", "count": "number of wallets"})

    attendance_freq_df = attendance_freq_df.sort_values(
        by=["number of attendance"], ascending=False)

    attendance_freq_df = attendance_freq_df.reset_index(drop=True)

    attendance_freq_df.to_csv(poap_attendance_frequency_file_name)


# calc transfer history metrics (transfers_before, transfers_after, average_transfers)
def calc_transfer_history_metrics(poap_transfer_df: pd.DataFrame, wallet_address_list):
    had_transaction_before = 0
    had_transaction_after = 0
    poap_owner = 0

    number_nfts_list = []

    for owner_address in wallet_address_list:
        poap_nft_owner_transfers_df = poap_transfer_df[(poap_transfer_df["from_address"] == owner_address) | (
            poap_transfer_df["to_address"] == owner_address)]

        poap_nft_owner_transfers_df["transaction_type"] = np.where(
            poap_nft_owner_transfers_df["to_address"] == owner_address, "buy", "sell")

        poap_nft_owner_transfers_df = poap_nft_owner_transfers_df.sort_values(
            by="block_number")

        if poap_contract_address in poap_nft_owner_transfers_df["token_address"].unique():
            buy_sell = poap_nft_owner_transfers_df["transaction_type"].to_list(
            )

            number_nfts = buy_sell.count("buy") - buy_sell.count("sell")

            number_nfts_list.append(number_nfts)

            poap_owner = poap_owner + 1

            poap_index = np.where(
                poap_nft_owner_transfers_df["token_address"] == poap_contract_address)[0][0]
            if poap_index > 0:
                had_transaction_before = had_transaction_before+1
            if poap_index < len(poap_nft_owner_transfers_df.index):
                had_transaction_after = had_transaction_after + 1

    transfers_before_perc = had_transaction_before / poap_owner
    transfers_after_perc = had_transaction_after / poap_owner
    avg_number_of_transfers = sum(number_nfts_list)/len(number_nfts_list)

    return transfers_before_perc, transfers_after_perc, avg_number_of_transfers


# save df to pkl
def save_df_to_pkl(df: pd.DataFrame, file_name):
    df.to_pickle(file_name)


# load df from pkl
def load_df_from_pkl(file_name):
    df = pd.read_pickle(file_name)
    return df


if __name__ == "__main__":
    db_cnx = BlockchainDatabaseConnector(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD,
        config.MYSQL_DB_NAME
    )

    poap_contract_address = config.POAP_CONTRACT_ADDRESS.lower()

    # file names
    poap_transfer_pkl_file_name = "datasets/poap_transfer_dataset.pkl"
    nft_balance_pkl_file_name = "datasets/nft_balance_dataset.pkl"
    poap_frequency_file_name = "results/poap_frequency.csv"
    nft_frequency_file_name = "results/nft_frequency_by_poap_holders.csv"
    nft_count_file_name = "results/nft_count_by_wallets.csv"
    poap_attendance_frequency_file_name = "results/poap_attendance_frequency.csv"

    # query initial data
    nft_data_df = db_cnx.query_nft_data(
        config.MYSQL_DB_TABLE_NAME_NFT)
    pfp_data_df = nft_data_df[nft_data_df["token_address"]
                              != poap_contract_address]
    poap_data_df = nft_data_df[nft_data_df["token_address"]
                               == poap_contract_address]
    insert_poap_name_from_metadata(poap_data_df)

    poap_wallets = poap_data_df["owner_of"].unique()

    # flags for data loading
    load_poap_transfers_from_db = True
    save_poap_transfer_to_pkl = True
    load_nft_balance_from_db = True
    save_nft_balance_to_pkl = True

    # load poap transfer data
    if load_poap_transfers_from_db:
        poap_transfer_df = db_cnx.query_nft_transfer_data(
            config.MYSQL_DB_TABLE_NAME_POAP_TRANSFER)

        if save_poap_transfer_to_pkl:
            save_df_to_pkl(poap_transfer_df, poap_transfer_pkl_file_name)
    else:
        poap_transfer_df = load_df_from_pkl(poap_transfer_pkl_file_name)

    # load nft balance data
    if load_nft_balance_from_db:
        nft_balance_df = db_cnx.query_nft_balance_data(
            config.MYSQL_DB_TABLE_NAME_NFT_BALANCE)

        if save_nft_balance_to_pkl:
            save_df_to_pkl(nft_balance_df, nft_balance_pkl_file_name)
    else:
        nft_balance_df = load_df_from_pkl(nft_balance_pkl_file_name)

    # common wallets
    common_wallets = get_common_pfp_poap_wallets(pfp_data_df, poap_data_df)
    print(f"Number of common wallets: {len(common_wallets)}")

    # calc poap frequency
    calc_poap_frequency(poap_data_df, poap_frequency_file_name)

    # calc nft frequency of poap wallets
    calc_nft_frequency_of_poap_wallets(
        nft_balance_df, poap_wallets, nft_frequency_file_name)

    # calc nft frequency by wallets
    average_number_of_tokens = calc_nft_count_by_wallet(
        nft_balance_df, common_wallets, nft_count_file_name)
    print(f"Average number of tokens hold: {average_number_of_tokens}")

    # calc attendance frequency
    calc_attendance_frequency(
        poap_data_df, poap_attendance_frequency_file_name)

    # calc tranfer history metrics
    transfers_before_perc, transfers_after_perc, avg_number_of_transfers = calc_transfer_history_metrics(
        poap_transfer_df, common_wallets)
    print(f"Perc of wallets with transfers before: {transfers_before_perc}")
    print(f"Perc of wallets with transfers after: {transfers_after_perc}")
    print(f"Average numver of transfers: {avg_number_of_transfers}")
