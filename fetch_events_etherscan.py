# Etape 1 pour Louis
# https://etherscan.io/api



#je mets ici le lien pour la documentation de l'API : https://docs.etherscan.io/make-your-first-call
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

#on va définir ce dont on a besoin pour récupérer les actions d'emprunts et de prêts 
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
BORROW_TOPIC0 = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0" #clé de hashage de l'action 

START_BLOCK = 16291127
END_BLOCK = 25218797


params = {
    "chainid": ETH_CHAIN_ID,
    "module": "logs",
    "action": "getLogs",   #on récupère les logs associés aux emprunts sur l'adresse Aave
    "address": AAVE_V3_POOL,
    "topic0": BORROW_TOPIC0,
    "fromBlock": START_BLOCK,
    "toBlock": END_BLOCK,
    "apikey": ETHERSCAN_API_KEY,
}
r = requests.get(ETHERSCAN_BASE_URL, params=params)
logs_etherscan = r.json()["result"]
print(f"[Etherscan] found {len(logs_etherscan)} Borrow events") #on regarde le nombre d'emprunts 
print(logs_etherscan)

#commande pour décoder 
print(int("0x180CEED", 16))

def decode(valeur):
    # Etherscan renvoie "0x" pour la valeur 0 
    return int(valeur, 16) if valeur not in ("0x", "") else 0

lignes = []
for log in logs_etherscan:
    data = log["data"][2:]                              # on enlève le "0x" du début

    lignes.append({
        "block_number": decode(log["blockNumber"]),
        "timestamp": decode(log["timeStamp"]),
        "tx_hash": decode["transactionHash"],
        "log_index": decode(log["logIndex"]),
        "reserve": "0x" + log["topics"][1][-40:],       # adresse du token emprunté
        "user": "0x" + data[24:64],                     # 1er bloc de 64 caractères
        "amount_raw": int(data[64:128], 16),            # 2e bloc de 64 caractères
    })

df_borrow = pd.DataFrame(lignes)
df_borrow