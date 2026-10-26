# Etape 1 pour Louis
# https://etherscan.io/api



#je mets ici le lien pour la documentation de l'API : https://docs.etherscan.io/make-your-first-call
import os
import time 
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
        "tx_hash": decode(log["transactionHash"]),
        "log_index": decode(log["logIndex"]),
        "reserve": "0x" + log["topics"][1][-40:],       # adresse du token emprunté
        "user": "0x" + data[24:64],                     # 1er bloc de 64 caractères
        "amount_raw": int(data[64:128], 16),            # 2e bloc de 64 caractères
    })

df_borrow = pd.DataFrame(lignes)
print(df_borrow)


#récupération des events borrow, supply, repay, liquidations, withdraw 

EVENTS= {
    "supply": "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
    "withdraw": "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7",
    "borrow": "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
    "repay": "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
    "liquidation": "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
}

def get_logs(topic0, start_block, end_block):
    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "logs",
        "action": "getLogs",
        "address": AAVE_V3_POOL,
        "topic0": topic0,
        "fromBlock": start_block,
        "toBlock": end_block,
        "apikey": ETHERSCAN_API_KEY,
    }
    r = requests.get(ETHERSCAN_BASE_URL, params=params)
    result = r.json()["result"]
    if not isinstance(result, list):      # en cas d'erreur, "result" est un texte
        raise RuntimeError(f"Erreur Etherscan : {result}")
    return result
    

def decode_log(event, log):
    data = log["data"][2:]       # on enlève le "0x"
    t = log["topics"]

    # Champs communs à tous les événements
    row = {
        "event": event,
        "block_number": decode(log["blockNumber"]),
        "timestamp": decode(log["timeStamp"]),
        "tx_hash": decode(log["transactionHash"]),
        "log_index": decode(log["logIndex"]),
    }

    return row

tables = {}
for event, topic0 in EVENTS.items():
    logs = get_logs(topic0, START_BLOCK, END_BLOCK)
    print(f"{event}: {len(logs)} logs")
    tables[event] = pd.DataFrame([decode_log(event, log) for log in logs])
    time.sleep(0.4)     # le plan gratuit limite à 3 requêtes/seconde
    print(tables)

tables["borrow"]         # un tableau par événement



#