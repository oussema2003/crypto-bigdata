# -*- coding: utf-8 -*-
# Configuration commune à tous les scripts du projet.
# Modifier ce fichier suffit pour changer les paires, les mois ou les adresses.

# Paires de cryptomonnaies suivies (format Binance, en majuscules)
PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT",
         "SOLUSDT", "LTCUSDT", "LINKUSDT", "TRXUSDT", "DOTUSDT", "AVAXUSDT"]

# Mois d'historique à télécharger (un fichier zip par paire et par mois) :
# 36 mois, de 2023-01 à 2025-12
MOIS = ["{}-{:02d}".format(a, m) for a in (2023, 2024, 2025) for m in range(1, 13)]
# Dossier HDFS qui contient l'historique : un sous-dossier par paire
HDFS_HISTORY = "/crypto/history/{pair}/*.csv"

# Fichier où le batch écrit les seuils et d'où le streaming les lit
SEUILS_FILE = "seuils.json"

# Quantile utilisé pour fixer le seuil : 0.99 = on alerte au-delà de ce que
# 99 % des mouvements de 1 minute n'atteignent pas. Le seuil est calculé
# séparément pour chaque paire et chaque heure UTC de la journée (24 par paire).
QUANTILE_SEUIL = 0.99

# Dossier (sur HDFS) où Spark note jusqu'où il a lu dans Kafka. Grâce à lui,
# un job arrêté puis relancé reprend sans perdre de messages.
# Pour repartir de zéro : hdfs dfs -rm -r /crypto/chk
CHECKPOINT_DIR = "/crypto/chk"

# Kafka : adresse du broker et nom du topic
KAFKA_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "klines"

# HBase : adresse du serveur Thrift (utilisé par happybase)
HBASE_HOST = "localhost"
HBASE_PORT = 9090

# Flux Binance : bougies d'une minute, une connexion pour toutes les paires
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
    p.lower() + "@kline_1m" for p in PAIRS
)