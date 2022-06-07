import requests
from time import sleep


class ApiConnector:

    def __init__(self, api_url, api_key, api_rate_limit) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.api_rate_limit = api_rate_limit
        self.headers = {"x-api-key": self.api_key}

    # request endpoint weights
    def request_endpoint_weights(self):
        response = requests.get(
            f"{self.api_url}//info/endpointWeights", headers=self.headers)
        return response

    # request nft data (owners) by contract_address
    def request_nft_data(self, contract_address, cursor=None, chain="eth", format="decimal", sleep_time=0.2):
        # build params
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor

        r_status_code = 0

        # request data
        while r_status_code != 200:
            response = requests.get(f"{self.api_url}/nft/{contract_address}/owners",
                                    headers=self.headers, params=params)
            r_status_code = response.status_code

            sleep(sleep_time)

        return response

    # request ft balance data by wallet_address
    def request_ft_balance_data(self, wallet_address, cursor=None, chain="eth", format="decimal", sleep_time=0.2):
        # build params
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor

        r_status_code = 0

        # request data
        while r_status_code != 200:
            response = requests.get(f"{self.api_url}/{wallet_address}/erc20",
                                    headers=self.headers, params=params)
            r_status_code = response.status_code

            sleep(sleep_time)

        return response

    # request nft balance data by wallet_address
    def request_nft_balance_data(self, wallet_address, cursor=None, chain="eth", format="decimal", sleep_time=0.2):
        # build params
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor

        r_status_code = 0

        # request data
        while r_status_code != 200:
            response = requests.get(f"{self.api_url}/{wallet_address}/nft",
                                    headers=self.headers, params=params)
            r_status_code = response.status_code

            sleep(sleep_time)

        return response

    # request ft transfer data by wallet_address
    def request_ft_transfer_data_by_wallet_address(self, wallet_address, cursor=None, chain="eth", format="decimal", sleep_time=0.2):
        # build params
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor

        r_status_code = 0

        # request data
        while r_status_code != 200:
            response = requests.get(f"{self.api_url}/{wallet_address}/erc20/transfers",
                                    headers=self.headers, params=params)
            r_status_code = response.status_code

            sleep(sleep_time)

        return response

    # request nft transfer data by wallet_address
    def request_nft_transfer_data_by_wallet_address(self, wallet_address, cursor=None, chain="eth", format="decimal", sleep_time=0.2):
        # build params
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor

        r_status_code = 0

        # request data
        while r_status_code != 200:
            response = requests.get(f"{self.api_url}/{wallet_address}/nft/transfers",
                                    headers=self.headers, params=params)
            r_status_code = response.status_code

            sleep(sleep_time)

        return response

    # request nft transfer data by contract_address
    def request_nft_transfer_data_by_contract_address(self, contract_address, cursor=None, chain="eth", format="decimal", sleep_time=0.2):
        # build params
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor

        r_status_code = 0

        # request data
        while r_status_code != 200:
            response = requests.get(f"{self.api_url}/nft/{contract_address}/transfers",
                                    headers=self.headers, params=params)
            r_status_code = response.status_code

            sleep(sleep_time)

        return response
