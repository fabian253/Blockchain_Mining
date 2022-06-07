from connectors.api_connector import ApiConnector
from connectors.blockchain_database_connector import BlockchainDatabaseConnector
import config
import pandas as pd


# request ft balance data per wallet from api
def request_ft_balance_data(api_cnx: ApiConnector, wallet_address):
    cursor = None

    ft_balance_list = list()

    r = api_cnx.request_ft_balance_data(wallet_address, cursor)

    for ft_data in r.json():
        ft_balance_list.append(ft_data)

    return ft_balance_list


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
    nft_wallet_list = list(dict.fromkeys(nft_wallet_list))

    # get distinct wallets for ft in db
    ft_wallet_db_list = db_cnx.query_distinct_wallets(
        config.MYSQL_DB_TABLE_NAME_FT_BALANCE)["wallet_address"].to_list()

    # get wallets that are not yet in db
    wallet_list = [
        wallet for wallet in nft_wallet_list if wallet not in ft_wallet_db_list]

    # request ft balance data for every wallet which is not in the db already
    for idx, wallet_address in enumerate(wallet_list):
        try:
            # request ft balance data
            ft_balance_data = request_ft_balance_data(api_cnx, wallet_address)

            # add information for wallet_address
            for ft_balance in ft_balance_data:
                ft_balance["owner_of"] = wallet_address

            # insert ft balance data into db
            db_cnx.insert_ft_balance_data(
                config.MYSQL_DB_TABLE_NAME_FT_BALANCE, ft_balance_data)

            print(
                f"Wallet {wallet_address} done [{idx +1}/{len(wallet_list)}]")
        except Exception as e:
            print(
                f"Wallet {wallet_address} error [{idx+1}/{len(wallet_list)}]")
