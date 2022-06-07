from connectors.api_connector import ApiConnector
from connectors.blockchain_database_connector import NftDatabaseConnector
import config
import pandas as pd


def query_nft_transfers(api_cnx: ApiConnector, owner_address):
    cursor = None

    nft_list = list()

    while True:
        r = api_cnx.request_nft_transfers_by_wallet_address(owner_address, cursor)

        for nft_data in r.json()["result"]:
            nft_list.append(nft_data)

        page = r.json()["page"]

        #print(f"Page {page} done")

        cursor = r.json()["cursor"]
        if cursor == "":
            break

    return nft_list


if __name__ == "__main__":
    db_cnx = NftDatabaseConnector(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD,
        config.MYSQL_DB_NAME
    )

    api_cnx = ApiConnector(config.MORALIS_API_URL, config.MORALIS_API_KEY,
                           config.MORALIS_API_RATE_LIMIT)

    poap_contract_address = "0x22C1f6050E56d2876009903609a2cC3fEf83B415"

    # contract addresses from api are always in lowercase
    poap_contract_address = poap_contract_address.lower()

    nft_data_df = db_cnx.query_all_nft_data()

    pfp_nft_df = nft_data_df[nft_data_df["token_address"]
                             != poap_contract_address]

    poap_nft_df = nft_data_df[nft_data_df["token_address"]
                              == poap_contract_address]

    pfp_unique_owners = pfp_nft_df["owner_of"].unique()

    poap_unique_owners = poap_nft_df["owner_of"].unique()

    with open("nft_transfers_collection_wallets.txt", "r") as f:
        lines = f.readlines()

    poap_owners_in_db = [line.split(" ")[1] for line in lines]


    common_owners = list(set(pfp_unique_owners).intersection(poap_unique_owners))

    common_owners = [owner for owner in common_owners if owner not in poap_owners_in_db]

    for idx, unique_owner in enumerate(common_owners):
        try:
            #print(f"Owner {unique_owner} started [{idx}/{len(unique_nft_owner_list)}]")
            nft_transfers = query_nft_transfers(api_cnx, unique_owner)

            db_cnx.insert_nft_data(nft_transfers, "poap_transfers")

            print(f"Owner {unique_owner} done [{idx}/{len(common_owners)}]")

        except Exception as e:
            print(f"Owner {unique_owner} error [{idx}/{len(common_owners)}]")
            raise e
