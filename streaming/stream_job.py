# -*- coding: utf-8 -*-
# STREAMING : Spark lit le topic Kafka, calcule le rendement de chaque bougie,
# le compare au seuil appris par le batch pour sa paire ET son heure (UTC),
# calcule la volatilité sur 5 minutes et écrit les résultats dans HBase.
#
# Lancement (dans le conteneur, depuis le dossier du projet). Adapter la
# version du paquet Kafka à celle de Spark (voir README) :
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
#       streaming/stream_job.py

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType,
                               StringType, StructField, StructType)

# Permet d'importer config.py et le dossier storage/ de la racine du projet
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import config

# Structure des messages JSON écrits par le producteur dans Kafka
SCHEMA = StructType([
    StructField("pair", StringType()),
    StructField("event_time", LongType()),
    StructField("open", DoubleType()),
    StructField("close", DoubleType()),
    StructField("closed", BooleanType()),
])


# Transforme les messages Kafka bruts (colonne 'value') en colonnes lisibles,
# ajoute l'heure (ts), l'heure de la journée en UTC (heure, de 0 à 23) et le
# rendement de la bougie : (close - open) / open
def parser(df_kafka):
    return (df_kafka
            .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"))
            .select("m.*")
            .withColumn("ts", (F.col("event_time") / 1000).cast("timestamp"))
            .withColumn("heure", F.hour("ts"))
            .withColumn("ret", (F.col("close") - F.col("open")) / F.col("open")))


# Joint chaque message au seuil de sa paire et de son heure, et ajoute la
# colonne 'alerte' (vraie quand |rendement| dépasse le seuil appris par le batch)
def ajouter_alerte(df, seuils_df):
    return (df.join(seuils_df, ["pair", "heure"])
            .withColumn("alerte", F.abs(F.col("ret")) > F.col("seuil")))


# Volatilité = écart-type des rendements des bougies terminées, sur une fenêtre
# de 5 minutes qui avance toutes les minutes
def volatilite(df):
    return (df.filter(F.col("closed"))
            .withWatermark("ts", "10 minutes")
            .groupBy(F.window("ts", "5 minutes", "1 minute"), "pair")
            .agg(F.stddev("ret").alias("vol"))
            .filter(F.col("vol").isNotNull() & ~F.isnan(F.col("vol"))))


# Appelée pour chaque micro-batch du flux principal : écrit chaque bougie dans
# la table 'prix' et, si elle dépasse le seuil, dans la table 'alertes'
def ecrire_prix_alertes(batch_df, batch_id):
    from storage import hbase_store
    prix, alertes = [], []
    for r in batch_df.collect():
        valeurs = {"close": r["close"], "ret": r["ret"],
                   "seuil": r["seuil"], "alerte": int(r["alerte"])}
        prix.append((r["pair"], r["event_time"], valeurs))
        if r["alerte"]:
            alertes.append((r["pair"], r["event_time"], valeurs))
    hbase_store.ecrire("prix", prix)
    hbase_store.ecrire("alertes", alertes)


# Appelée pour chaque micro-batch du flux de volatilité : écrit dans 'volatilite'
def ecrire_volatilite(batch_df, batch_id):
    from storage import hbase_store
    lignes = []
    for r in batch_df.collect():
        fin_ms = int(r["window"]["end"].timestamp() * 1000)
        lignes.append((r["pair"], fin_ms, {"vol": r["vol"]}))
    hbase_store.ecrire("volatilite", lignes)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("StreamCrypto").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Même fuseau que le batch : l'heure de la journée est en UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Charge les seuils calculés par le batch (24 par paire : un par heure)
    # et en fait un petit DataFrame (pair, heure, seuil)
    with open(config.SEUILS_FILE) as f:
        seuils = json.load(f)
    lignes = []
    for pair, par_heure in seuils.items():
        if not isinstance(par_heure, dict):
            sys.exit("seuils.json est à l'ancien format : relancer batch/batch_seuils.py")
        for heure, seuil in par_heure.items():
            lignes.append((pair, int(heure), float(seuil)))
    seuils_df = spark.createDataFrame(lignes, "pair string, heure int, seuil double")

    # Lecture du topic Kafka en continu (uniquement les nouveaux messages)
    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", config.KAFKA_SERVERS)
             .option("subscribe", config.KAFKA_TOPIC)
             .option("startingOffsets", "latest")
             .load())
    flux = parser(kafka)

    # Flux 1 : prix + alertes, un micro-batch toutes les 5 secondes.
    # checkpointLocation : mémorise la position de lecture dans Kafka
    (ajouter_alerte(flux, seuils_df).writeStream
     .foreachBatch(ecrire_prix_alertes)
     .option("checkpointLocation", config.CHECKPOINT_DIR + "/prix")
     .trigger(processingTime="5 seconds")
     .start())

    # Flux 2 : volatilité sur 5 minutes glissantes
    (volatilite(flux).writeStream
     .outputMode("update")
     .foreachBatch(ecrire_volatilite)
     .option("checkpointLocation", config.CHECKPOINT_DIR + "/volatilite")
     .trigger(processingTime="5 seconds")
     .start())

    # Attend la fin des deux flux (Ctrl+C pour arrêter)
    spark.streams.awaitAnyTermination()