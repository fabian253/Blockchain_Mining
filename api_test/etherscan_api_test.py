import requests
import json
import config

module = "stats"
action = "tokensupply"
contract_address = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"


request_url = "https://api.etherscan.io/api"

pload = {"module": module, "action": action,
         "contractaddress": contract_address, "apikey": config.ETHERSCAN_API_KEY}

r = requests.get(request_url, pload)

with open("test.json", "w") as f:
    json.dump(r.json(), f)
