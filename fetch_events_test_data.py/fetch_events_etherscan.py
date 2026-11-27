# Etape 1 pour Louis
# https://etherscan.io/api

#Récupération des données et production des CSV 

#je mets ici le lien pour la documentation de l'API : https://docs.etherscan.io/make-your-first-call

import os # pour lire les variables d'environnement (la clé d'API, le nom du bucket SSP Cloud)
import time   # pour faire des pauses entre les appels à l'API (limite de 3 requêtes par seconde) 

#l'objectif ici est de récupérer la clé API qui est sécurisée dans un dossier env 
if "ETHERSCAN_API_KEY" not in globals():
    from dotenv import load_dotenv
    load_dotenv()
    ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

assert ETHERSCAN_API_KEY not in (None, "your-actual-etherscan-api-key"), "Set your Etherscan key (.env)" #on vérifie qu'on a bien la clé 
print("Key loaded.")

# on importe les bibliothèques utiles 

import requests          # pour envoyer les requêtes à l'API d'Etherscan et recevoir les réponses
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
etherscan_block = int(r.json()["result"], 16) #ici on traduit juste le résultat car il est en r.json
print(r.json())
print("Etherscan says latest block is:", etherscan_block)

#on va définir ce dont on a besoin pour récupérer les actions d'emprunts et de prêts 
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
BORROW_TOPIC0 = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0" #clé de hashage de l'action 

START_BLOCK = 16291127 #bloc du début de notre étude 
END_BLOCK = 25218797    #bloc de fin 

#Récupération des donées de Borrow dans un premier temps 

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

#on a rencontré un problème avec le code précédent, il a donc fallu en refaire un plus astucieux 
#on divise par deux à chaque fois que ça sature 



PAGE = 1000     # nombre maximal de résultats renvoyés par Etherscan en un appel

def decode(valeur):
    return int(valeur, 16) if valeur not in ("0x", "") else 0

def etherscan_get(params, retries=5):       # envoie une requête à Etherscan et recommence jusqu'à 5 fois en cas d'erreur
    for attempt in range(retries):
        try:
            result = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=15).json().get("result")
        except requests.exceptions.RequestException as err:   
            result = f"erreur réseau : {err}"
        if isinstance(result, list):  #on teste si c'est une liste 
            time.sleep(0.35)        ## pause pour respecter la limite de 3 requêtes par seconde
            return result
        print(f"  tentative {attempt + 1}/{retries} échouée : {result}", flush=True)
        time.sleep(1 + attempt)
    raise RuntimeError(f"Erreur Etherscan : {result}")

def fetch_logs(topic0, start, end):
    params = {
        "chainid": ETH_CHAIN_ID, "module": "logs", "action": "getLogs",
        "address": AAVE_V3_POOL, "topic0": topic0,
        "fromBlock": start, "toBlock": end,
        "page": 1, "offset": PAGE,
        "apikey": ETHERSCAN_API_KEY,
    }
    logs = etherscan_get(params)
    if len(logs) < PAGE:                # moins de 1 000 résultats : l'intervalle est complet
        return logs
    mid = (start + end) // 2                # sinon, on coupe l'intervalle en deux
    return fetch_logs(topic0, start, mid) + fetch_logs(topic0, mid + 1, end). #et on recommence sur chaque moitié
 

def addr(x):
    return "0x" + x[-40:]       # une adresse = les 40 derniers caractères

def decode_log(event, log):
    data = log["data"][2:]                                   # on enlève le "0x"
    w = [data[i:i+64] for i in range(0, len(data), 64)]      # data découpé en mots de 32 octets
    t = log["topics"]

    row = {                                                  # on stocke dans une variable.
        "event": event,
        "block_number": decode(log["blockNumber"]),
        "timestamp": decode(log["timeStamp"]),
        "tx_hash": log["transactionHash"],
        "log_index": decode(log["logIndex"]),
    }
# la place de chaque information dépend du type d'événement (voir la documentation d'Aave)
    if event in ("supply", "borrow"):                        
        row["reserve"] = addr(t[1])
        row["user"] = addr(w[0])
        row["amount_raw"] = int(w[1], 16)

    elif event in ("withdraw", "repay"):
        row["reserve"] = addr(t[1])
        row["user"] = addr(t[2])
        row["amount_raw"] = int(w[0], 16)

    elif event == "liquidation":
        row["user"] = addr(t[3])
        row["debt_to_cover_raw"] = int(w[0], 16)
        row["liquidator"] = addr(w[2])

    return row                                               # on return à la fin 

# pour chaque type d'événement : collecte complète, décodage, puis enregistrement en CSV
tables = {}
for event, topic0 in EVENTS.items():
    logs = fetch_logs(topic0, START_BLOCK, END_BLOCK)    
    print(f"{event}: {len(logs)} logs")
    tables[event] = pd.DataFrame([decode_log(event, log) for log in logs])
    tables[event].to_csv(f"aave_{event}.csv", index=False)
print(tables)