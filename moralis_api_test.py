from database_connector import NftDatabaseConnector
import requests
import config
import json

url = "https://deep-index.moralis.io/api/v2"
headers = {"x-api-key": config.MORALIS_API_KEY}
contract_address = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"


def insert_nfts(cnx: NftDatabaseConnector, http_reponse):
    for nft in http_reponse.json()["result"]:
        cnx.insert_nft("nft", nft)


def request_nfts(contract_address, cursor=None):
    params = {"chain": "eth", "format": "decimal"}
    if cursor is not None:
        params["cursor"] = cursor
    response = requests.get(f"{url}/nft/{contract_address}/owners",
                            headers=headers, params=params)
    return response


if __name__ == "__main__":
    cnx = NftDatabaseConnector()
    cnx.connect(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD
    )
    cnx.use_database("blockchain_mining")

    cursor = None

    while True:
        r = request_nfts(contract_address, cursor)
        insert_nfts(cnx, r)

        page = r.json()["page"]

        with open(f"data/data_{page}.json", "w") as f:
            json.dump(r.json(), f)

        cursor = r.json()["cursor"]
        if cursor == "":
            break

    cnx.disconnect()
