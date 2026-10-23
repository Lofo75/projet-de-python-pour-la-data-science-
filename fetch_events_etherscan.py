# Etape 1 pour Louis
# https://etherscan.io/api

import os

#l'objectif ici est de récupérer la clé API qui est sécurisée dans un dossier env 
if "ETHERSCAN_API_KEY" not in globals():
    from dotenv import load_dotenv
    load_dotenv()
    ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

assert ETHERSCAN_API_KEY not in (None, "your-actual-etherscan-api-key"), "Set your Etherscan key (.env)"
print("Key loaded.")

# on importe les bibliothèques utiles 

import requests
import pandas as pd
from datetime import datetime, timezone


ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1 #car etherscan gère plusieurs blockchain avec la même URL 


# on teste si l'environnement fonctionne bien en récupérant le dernier bloc 
params = {
    "chainid": ETH_CHAIN_ID,
    "module": "proxy",
    "action": "eth_blockNumber",
    "apikey": ETHERSCAN_API_KEY,
}
r = requests.get(ETHERSCAN_BASE_URL, params=params) #on envoie la requête
etherscan_block = int(r.json()["result"], 16) #ici on traduit juste le résultat 
print(r.json())
print("Etherscan says latest block is:", etherscan_block)
