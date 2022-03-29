import requests


class ApiConnector:

    def __init__(self, api_url, api_key) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.headers = {"x-api-key": self.api_key}

    def request_nft_owners(self, contract_address, cursor=None, chain="eth", format="decimal"):
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor
        response = requests.get(f"{self.api_url}/nft/{contract_address}/owners",
                                headers=self.headers, params=params)
        return response

    def request_nft_transfers(self, contract_address, cursor=None, chain="eth", format="decimal"):
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor
        response = requests.get(f"{self.api_url}/nft/{contract_address}/transfers",
                                headers=self.headers, params=params)
        return response
