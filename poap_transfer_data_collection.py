from connectors.api_connector import ApiConnector
from connectors.blockchain_database_connector import BlockchainDatabaseConnector
import config
import os


# request nft transfer data per wallet from api
def request_nft_transfer_data(api_cnx: ApiConnector, wallet_address):
    cursor = None

    nft_transfer_list = list()

    while True:
        r = api_cnx.request_nft_transfer_data_by_wallet_address(
            wallet_address, cursor)

        for nft_transfer_data in r.json()["result"]:
            nft_transfer_list.append(nft_transfer_data)

        #page = r.json()["page"]

        #print(f"Page {page} done")

        cursor = r.json()["cursor"]
        if cursor == "" or cursor == None:
            break

    return nft_transfer_list


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

    poap_contract_address = config.POAP_CONTRACT_ADDRESS

    # contract addresses from api are always in lowercase
    poap_contract_address = poap_contract_address.lower()

    # query all nft data
    nft_data_df = db_cnx.query_nft_data(config.MYSQL_DB_TABLE_NAME_NFT)
    # filter for pfp collections
    pfp_data_df = nft_data_df[nft_data_df["token_address"]
                              != poap_contract_address]
    # filter for poap collections
    poap_data_df = nft_data_df[nft_data_df["token_address"]
                               == poap_contract_address]

    # get unique pfp wallets
    pfp_wallets = pfp_data_df["owner_of"].unique()
    # get unique poap wallets
    poap_wallets = poap_data_df["owner_of"].unique()

    # get common pfp and poap wallets
    common_wallets = [
        wallet for wallet in pfp_wallets if wallet in poap_wallets]

    # read wallets from log file
    if os.path.exists("logs/poap_transfer_data_collection_log.txt"):
        with open("logs/poap_transfer_data_collection_log.txt", "r") as log_file:
            db_wallets = log_file.readlines()
            db_wallets = [wallet.replace("\n", "") for wallet in db_wallets]
        # filter wallets for wallets that are already in db
        common_wallets = [
            wallet for wallet in common_wallets if wallet not in db_wallets]
    
    # remove dead address
    dead_address = "0x000000000000000000000000000000000000dead"
    if dead_address in common_wallets:
        common_wallets.remove(dead_address)

    # request nft transfer data for every common wallet
    for idx, wallet_address in enumerate(common_wallets):
        try:
            # request nft transfer data
            nft_transfer_data = request_nft_transfer_data(
                api_cnx, wallet_address)

            # insert nft balance data into db
            db_cnx.insert_nft_transfer_data(
                config.MYSQL_DB_TABLE_NAME_POAP_TRANSFER, nft_transfer_data)

            # write wallet address to log file
            with open("logs/poap_transfer_data_collection_log.txt", "a") as log_file:
                log_file.write(f"\n{wallet_address}")

            print(
                f"Wallet {wallet_address} done [{idx +1}/{len(common_wallets)}]")
        except Exception as e:
            print(
                f"Wallet {wallet_address} error [{idx+1}/{len(common_wallets)}]")
