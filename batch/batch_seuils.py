# -*- coding: utf-8 -*-
# BATCH : lit l'historique sur HDFS, mesure la volatilité et fixe 24 seuils
# d'alerte par paire : un par heure de la journée (0 à 23, heure UTC).
# Le résultat est écrit dans seuils.json.
#
# Définition du rendement (la même dans le batch et dans le streaming) :
#   rendement d'une bougie 1 minute = (close - open) / open
#
# Lancement (dans le conteneur, depuis le dossier du projet) :
#   spark-submit batch/batch_seuils.py

import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Permet d'importer config.py qui est à la racine du projet
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import config

# Démarre la session Spark
spark = SparkSession.builder.appName("BatchSeuils").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Les heures sont calculées en UTC (comme les horodatages Binance), de la même
# façon dans le batch et dans le streaming
spark.conf.set("spark.sql.session.timeZone", "UTC")

seuils = {}

for pair in config.PAIRS:
    print("=" * 60)
    print("Paire :", pair)

    # Lecture des CSV de la paire. Les fichiers Binance n'ont pas d'en-tête :
    # colonnes _c0 = open_time, _c1 = open, _c2 = high, _c3 = low, _c4 = close
    brut = spark.read.csv(config.HDFS_HISTORY.format(pair=pair), header=False, inferSchema=True)

    # Garde les colonnes utiles. Depuis 2025 l'horodatage des fichiers spot est
    # en microsecondes (avant : millisecondes) : on convertit tout en secondes.
    df = brut.select(
        F.when(F.col("_c0") > 10 ** 14, F.col("_c0") / 1000000)
         .otherwise(F.col("_c0") / 1000).cast("timestamp").alias("ts"),
        F.col("_c1").cast("double").alias("open"),
        F.col("_c4").cast("double").alias("close"),
    )

    # Rendement de chaque minute
    df = df.withColumn("ret", (F.col("close") - F.col("open")) / F.col("open"))
    df.createOrReplaceTempView("klines")

    # Requête 1 : un seuil par heure de la journée (0 à 23, UTC)
    #   seuil(paire, h) = quantile 99 % de |rendement| sur les minutes de l'heure h
    lignes = spark.sql(
        "SELECT hour(ts) AS heure, percentile_approx(abs(ret), {q}) AS seuil, "
        "count(*) AS n FROM klines GROUP BY hour(ts) ORDER BY heure".format(q=config.QUANTILE_SEUIL)
    ).collect()
    seuils[pair] = {str(l["heure"]): float(l["seuil"]) for l in lignes}
    print("Seuil d'alerte par heure UTC (|rendement| 1 min) :")
    for l in lignes:
        print("  {:02d} h : {:.6f}  ({} bougies)".format(l["heure"], l["seuil"], l["n"]))

    # Requête 2 : volatilité globale de la paire sur tout l'historique
    glob = spark.sql("SELECT stddev(ret) AS volatilite, count(*) AS n FROM klines").collect()[0]
    print("Bougies lues :", glob["n"])
    print("Volatilité globale (écart-type) :", glob["volatilite"])

    # Requête 3 : les 5 heures les plus volatiles de l'historique
    print("Les 5 heures les plus volatiles :")
    spark.sql(
        "SELECT date_format(ts, 'yyyy-MM-dd HH') AS heure, stddev(ret) AS volatilite "
        "FROM klines GROUP BY date_format(ts, 'yyyy-MM-dd HH') "
        "ORDER BY volatilite DESC LIMIT 5"
    ).show(truncate=False)

    # Requête 4 : les 5 plus gros mouvements d'une minute
    print("Les 5 plus gros mouvements d'une minute :")
    spark.sql(
        "SELECT ts, open, close, ret FROM klines ORDER BY abs(ret) DESC LIMIT 5"
    ).show(truncate=False)

# Écrit les seuils dans un fichier JSON : le job streaming les relira
with open(config.SEUILS_FILE, "w") as f:
    json.dump(seuils, f, indent=2)
print("Seuils enregistrés dans", config.SEUILS_FILE, ":", seuils)

spark.stop()