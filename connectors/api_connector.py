import requests
import datetime
from time import sleep


class ApiConnector:

    def __init__(self, api_url, api_key, api_rate_limit) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.api_rate_limit = api_rate_limit
        self.headers = {"x-api-key": self.api_key}

    #TODO create working version
    def rate_limit_decorator(func):
        def rate_limit_wrapper(self, *args, **kwargs):
            start_time = datetime.datetime.now()
            value = func(self, *args, **kwargs)
            end_time = datetime.datetime.now()
            execution_time = (end_time-start_time).total_seconds()*1000

            print(execution_time)

            if execution_time < 1000/(self.api_rate_limit/5):
                wait_time = (1000/(self.api_rate_limit/5) -
                             execution_time+1)/1000
                print(f"waittime: {wait_time}")
                sleep(wait_time)

            return value

        return rate_limit_wrapper
    

    def request_endpoint_weights(self):
        response = requests.get(
            f"{self.api_url}//info/endpointWeights", headers=self.headers)
        return response

    def request_nft_owners(self, contract_address, cursor=None, chain="eth", format="decimal"):
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor
        response = requests.get(f"{self.api_url}/nft/{contract_address}/owners",
                                headers=self.headers, params=params)
        sleep(1)
        return response

    def request_nft_transfers(self, contract_address, cursor=None, chain="eth", format="decimal"):
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor
        response = requests.get(f"{self.api_url}/nft/{contract_address}/transfers",
                                headers=self.headers, params=params)
        sleep(1)
        return response
    
    def request_erc20_transfers(self, wallet_address, cursor=None, chain="eth", format="decimal"):
        params = {"chain": chain, "format": format}
        if cursor is not None:
            params["cursor"] = cursor
        response = requests.get(f"{self.api_url}/{wallet_address}/erc20/transfers",
                                headers=self.headers, params=params)
        if response.status_code != 200:
            sleep(0.05)
            response = requests.get(f"{self.api_url}/{wallet_address}/erc20/transfers",
                                headers=self.headers, params=params)
        return response
