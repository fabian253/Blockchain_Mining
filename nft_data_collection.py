from connectors.api_connector import ApiConnector
from connectors.blockchain_database_connector import BlockchainDatabaseConnector
import config
import pandas as pd


# request nft data per contract from api
def request_nft_data(api_cnx: ApiConnector, contract_address):
    cursor = None

    nft_data_list = list()

    while True:
        r = api_cnx.request_nft_data(contract_address, cursor)

        for nft_data in r.json()["result"]:
            nft_data_list.append(nft_data)

        #page = r.json()["page"]

        #print(f"Page {page} done")

        cursor = r.json()["cursor"]
        if cursor == "" or cursor == None:
            break

    return nft_data_list


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

    # request nft data for every contract_address
    for idx, contract_address in enumerate(pfp_contract_address_list):
        try:
            # request nft data
            nft_data = request_nft_data(api_cnx, contract_address)

            # insert nft balance data into db
            db_cnx.insert_nft_data(
                config.MYSQL_DB_TABLE_NAME_NFT, nft_data)

            print(
                f"Contract {contract_address} done [{idx +1}/{len(pfp_contract_address_list)}]")
        except Exception as e:
            print(
                f"Contract {contract_address} error [{idx+1}/{len(pfp_contract_address_list)}]")
