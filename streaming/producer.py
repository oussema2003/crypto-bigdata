# -*- coding: utf-8 -*-
# PRODUCTEUR : lit le flux WebSocket de Binance (bougies 1 minute) et publie
# chaque message dans le topic Kafka.
#
# Lancement :
#   python streaming/producer.py          -> flux réel Binance
#   python streaming/producer.py simu     -> flux simulé (si Binance est bloqué)

import json
import os
import random
import sys
import time

from kafka import KafkaProducer

# Permet d'importer config.py qui est à la racine du projet
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import config

# Connexion à Kafka : les messages sont envoyés au format JSON
producteur = KafkaProducer(
    bootstrap_servers=config.KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# Envoie un message simplifié dans Kafka :
# paire, heure de l'événement (ms), prix d'ouverture, prix actuel, bougie fermée ?
def envoyer(pair, event_time, open_, close, closed):
    message = {"pair": pair, "event_time": int(event_time),
               "open": float(open_), "close": float(close), "closed": bool(closed)}
    producteur.send(config.KAFKA_TOPIC, message)


# Mode réel : appelé à chaque message reçu de Binance
def mode_reel():
    import websocket  # bibliothèque websocket-client

    # Binance enveloppe chaque message : {"stream": ..., "data": {...}}
    # data["k"] contient la bougie : o = open, c = close, x = bougie terminée
    def sur_message(ws, texte):
        data = json.loads(texte)["data"]
        k = data["k"]
        envoyer(data["s"], data["E"], k["o"], k["c"], k["x"])

    def sur_erreur(ws, erreur):
        print("Erreur WebSocket :", erreur)

    # Boucle de reconnexion : si la connexion tombe, on réessaie après 5 s
    while True:
        ws = websocket.WebSocketApp(config.BINANCE_WS, on_message=sur_message, on_error=sur_erreur)
        ws.run_forever()
        print("Connexion perdue, nouvelle tentative dans 5 s")
        time.sleep(5)


# Mode simulation : marche aléatoire autour d'un prix de départ, avec de temps
# en temps un gros mouvement pour déclencher des alertes
def mode_simu():
    prix = {p: 100.0 for p in config.PAIRS}
    ouverture = dict(prix)
    compteur = 0
    while True:
        compteur += 1
        for pair in config.PAIRS:
            # petit bruit, et un choc de 2 % une fois sur 40
            variation = random.gauss(0, 0.0005)
            if random.random() < 0.025:
                variation += random.choice([-0.02, 0.02])
            prix[pair] *= 1 + variation
            # une bougie dure 12 messages de 5 s = 1 minute simulée
            fermee = compteur % 12 == 0
            envoyer(pair, time.time() * 1000, ouverture[pair], prix[pair], fermee)
            if fermee:
                ouverture[pair] = prix[pair]
        producteur.flush()
        time.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "simu":
        print("Mode simulation")
        mode_simu()
    else:
        print("Mode réel : connexion à Binance")
        mode_reel()
