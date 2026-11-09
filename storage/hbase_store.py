# -*- coding: utf-8 -*-
# STOCKAGE : lecture et écriture dans HBase via happybase (serveur Thrift).
# Utilisé par le job streaming (écriture) et par le dashboard (lecture).
#
# Clé de ligne (rowkey) = PAIRE#horodatage inversé
# L'horodatage est inversé (9999999999999 - ts) pour que la ligne la plus
# RÉCENTE d'une paire soit la PREMIÈRE lue par un scan.

import happybase

import config

# Famille de colonnes unique, créée avec les tables
FAMILLE = "d:"


# Ouvre une connexion vers le serveur Thrift de HBase
def connexion():
    return happybase.Connection(config.HBASE_HOST, config.HBASE_PORT)


# Construit la clé de ligne à partir de la paire et du temps (en millisecondes)
def cle(pair, ts_ms):
    return "{}#{:013d}".format(pair, 9999999999999 - int(ts_ms))


# Transforme un dictionnaire de valeurs en colonnes HBase (tout en octets)
def _colonnes(valeurs):
    return {(FAMILLE + k).encode(): str(v).encode() for k, v in valeurs.items()}


# Écrit une liste de lignes dans une table. Chaque ligne = (pair, ts_ms, dict)
def ecrire(table, lignes):
    if not lignes:
        return
    con = connexion()
    try:
        with con.table(table).batch() as b:
            for pair, ts_ms, valeurs in lignes:
                b.put(cle(pair, ts_ms).encode(), _colonnes(valeurs))
    finally:
        con.close()


# Lit les N lignes les plus récentes d'une paire. Retourne une liste de dict
# (la plus récente en premier).
def lire(table, pair, limite=100):
    con = connexion()
    try:
        resultat = []
        for cle_ligne, colonnes in con.table(table).scan(
                row_prefix=(pair + "#").encode(), limit=limite):
            ligne = {k.decode().replace(FAMILLE, ""): v.decode() for k, v in colonnes.items()}
            # on retrouve le temps (ms) à partir de la clé inversée
            ligne["ts_ms"] = 9999999999999 - int(cle_ligne.decode().split("#")[1])
            resultat.append(ligne)
        return resultat
    finally:
        con.close()
