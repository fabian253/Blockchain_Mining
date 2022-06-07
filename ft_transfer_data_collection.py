from connectors.api_connector import ApiConnector
from connectors.blockchain_database_connector import BlockchainDatabaseConnector
import config
import pandas as pd


# request ft transfer data per wallet from api
def request_ft_transfer_data(api_cnx: ApiConnector, wallet_address):
    cursor = None

    ft_transfer_list = list()

    while True:
        r = api_cnx.request_ft_transfer_data_by_wallet_address(
            wallet_address, cursor)

        for ft_transfer_data in r.json()["result"]:
            ft_transfer_list.append(ft_transfer_data)

        #page = r.json()["page"]

        #print(f"Page {page} done")

        cursor = r.json()["cursor"]
        if cursor == "":
            break

    return ft_transfer_list


if __name__ == "__main__":
    db_cnx = BlockchainDatabaseConnector(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD,
        config.MYSQL_DB_NAME
    )

    api_cnx = ApiConnector(
        config.MORALIS_API_URL,
        config.MORALIS_API_KEY,
        config.MORALIS_API_RATE_LIMIT)

    # get pfp contract addresses
    pfp_contract_address_list = pd.read_csv(
        "datasets/pfp_collections.csv")["contract_address"].to_list()

    nft_wallet_list = list()

    # get distinct wallets for each pfp contract address
    for contract_address in pfp_contract_address_list:
        distict_wallets = db_cnx.query_distinct_wallets(
            config.MYSQL_DB_TABLE_NAME_NFT, contract_address)["wallet_address"].to_list()

        nft_wallet_list.extend(distict_wallets)

    # remove duplicates
    wallet_list = list(dict.fromkeys(nft_wallet_list))

    # request ft transfer data for every common wallet
    for idx, wallet_address in enumerate(wallet_list):
        try:
            # request nft transfer data
            ft_transfer_data = request_ft_transfer_data(
                api_cnx, wallet_address)

            # insert nft balance data into db
            db_cnx.inser_ft_transfer_data(
                config.MYSQL_DB_TABLE_NAME_FT_TRANSFER, ft_transfer_data)

            print(
                f"Wallet {wallet_address} done [{idx +1}/{len(wallet_list)}]")
        except Exception as e:
            print(
                f"Wallet {wallet_address} error [{idx+1}/{len(wallet_list)}]")
