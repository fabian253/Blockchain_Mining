from database_connector import NftDatabaseConnector
from api_connector import ApiConnector
import config
import json

api_url = "https://deep-index.moralis.io/api/v2"
contract_address = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"


def insert_nfts(cnx: NftDatabaseConnector, http_reponse):
    for nft in http_reponse.json()["result"]:
        cnx.insert_value("nft", nft)


def insert_nft_transfers(cnx: NftDatabaseConnector, http_reponse):
    for transfer in http_reponse.json()["result"]:
        cnx.insert_value("transfers", transfer)


def query_all_nft_owners():
    db_cnx = NftDatabaseConnector()
    db_cnx.connect(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD
    )
    db_cnx.use_database("blockchain_mining")

    api_cnx = ApiConnector(api_url, config.MORALIS_API_KEY)

    cursor = None

    while True:
        r = api_cnx.request_nft_owners(contract_address, cursor)
        insert_nfts(db_cnx, r)

        page = r.json()["page"]

        print(f"Page {page} done")

        with open(f"data/data_{page}.json", "w") as f:
            json.dump(r.json(), f)

        cursor = r.json()["cursor"]
        if cursor == "":
            break

    db_cnx.disconnect()


def query_all_nft_transfers():
    db_cnx = NftDatabaseConnector()
    db_cnx.connect(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD
    )
    db_cnx.use_database("blockchain_mining")

    api_cnx = ApiConnector(api_url, config.MORALIS_API_KEY)

    cursor = None

    while True:
        r = api_cnx.request_nft_transfers(contract_address, cursor)
        insert_nft_transfers(db_cnx, r)

        page = r.json()["page"]

        print(f"Page {page} done")

        #with open(f"data/data_{page}.json", "w") as f:
        #    json.dump(r.json(), f)

        cursor = r.json()["cursor"]
        if cursor == "":
            break

    db_cnx.disconnect()


if __name__ == "__main__":
    query_all_nft_transfers()
