from collections import defaultdict
from decimal import Decimal
import requests
import config

# see: https://ethereum.stackexchange.com/questions/41684/api-to-gather-list-of-top-token-holders
# see: https://gist.github.com/rokcarl/96a06ab1faf9dfdb0f946aec72ab5ce8


network = "mainnet"


def get_rpc_response(method, params=[]):
    url = f"https://{network}.infura.io/v3/{config.INFURA_PROJECT_ID}"
    params = params or []
    data = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=data)
    return response.json()


def get_contract_transfers(address, decimals=18, from_block=None):
    """Get logs of Transfer events of a contract"""
    from_block = from_block or "0x0"
    transfer_hash = "0x913d4fe8c6e9eb51130abd46bf14fd379059a795a54b215ddd9b45cf48e5abc5"
    params = [{"address": address, "fromBlock": from_block,
               "topics": [transfer_hash]}]
    logs = get_rpc_response("eth_getLogs", params)  # ['result']
    from pprint import pprint as pp
    pp(logs[100])
    decimals_factor = Decimal("10") ** Decimal("-{}".format(decimals))
    for log in logs:
        log["amount"] = Decimal(str(int(log["data"], 16))) * decimals_factor
        log["from"] = log["topics"][1][0:2] + log["topics"][1][26:]
        log["to"] = log["topics"][2][0:2] + log["topics"][2][26:]
    return logs


def get_balances(transfers):
    balances = defaultdict(Decimal)
    for t in transfers:
        balances[t["from"]] -= t["amount"]
        balances[t["to"]] += t["amount"]
    bottom_limit = Decimal("0.00000000001")
    balances = {k: balances[k] for k in balances if balances[k] > bottom_limit}
    return balances


def get_balances_list(transfers):
    balances = get_balances(transfers)
    balances = [{"address": a, "amount": b} for a, b in balances.items()]
    balances = sorted(balances, key=lambda b: -abs(b["amount"]))
    return balances


contract = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
transfers = get_contract_transfers(contract, from_block=14397164)
balances = get_balances_list(transfers)

print(balances)
