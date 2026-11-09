# Volatilité des cryptomonnaies : batch + temps réel

Projet Big Data (II3) : détecter en direct les mouvements anormaux de prix de 12 cryptomonnaies
à partir d'un seuil d'alerte calculé sur l'historique.

- **Batch :** historique Binance (2023-2025) sur HDFS -> volatilité et seuil d'alerte par paire (Spark SQL).
- **Temps réel :** WebSocket Binance -> Kafka -> Spark Structured Streaming -> HBase -> interface Streamlit.

## 1. Problème

Les cryptomonnaies sont très volatiles : un mouvement de 0,3 % en une minute peut être normal
pour une paire et exceptionnel pour une autre. Un seuil fixe ne convient donc pas. Le projet répond à deux questions :

1. **Quel est un mouvement « anormal » pour chaque paire ?** Réponse par le batch, qui analyse des millions de bougies historiques.
2. **Cet instant est-il anormal maintenant ?** Réponse par le streaming, qui compare chaque nouvelle bougie au seuil de sa paire.

Définition du rendement (identique dans le batch et le streaming) :
`rendement d'une bougie 1 minute = (close - open) / open`

Une **alerte** est levée quand `|rendement| > seuil` de la paire. Le seuil est le **quantile 99 %**
de `|rendement|` sur tout l'historique (paramètre `QUANTILE_SEUIL` dans `config.py`) : environ 1 bougie sur 100 déclenche une alerte.

## 2. Architecture

```
            BATCH (historique)                              TEMPS RÉEL
  Binance data.binance.vision (CSV 1 min)          Binance WebSocket (kline_1m)
              |                                               |
        telecharger_historique.sh                        producer.py
              |                                               |
        HDFS /crypto/history                           Kafka (topic klines)
              |                                               |
        batch_seuils.py (Spark SQL)                   stream_job.py (Spark Structured Streaming)
              |                                               |
         seuils.json  ------ seuil par paire ------->  rendement, alerte, volatilité
                                                              |
                                                     HBase (prix, alertes, volatilite)
                                                              |
                                                     dashboard.py (Streamlit)
```

Le fichier `seuils.json` fait le lien entre les deux mondes : le batch le produit, le streaming le lit.

## 3. Technologies et justification

| Technologie | Rôle | Pourquoi |
|---|---|---|
| **HDFS** | stockage de l'historique | volume important (12 paires x 36 mois x ~43 000 bougies/mois), distribué et répliqué |
| **Spark SQL** (batch) | rendement, volatilité, quantile 99 % | calcul parallèle sur tout l'historique, `percentile_approx` natif |
| **Kafka** | bus de messages | découple la source (WebSocket) du traitement, tamponne les rafales, rejouable |
| **Spark Structured Streaming** | traitement en continu | même moteur et mêmes fonctions que le batch, fenêtres et état gérés, tolérant aux pannes (checkpoint) |
| **HBase** | stockage temps réel | écritures rapides, lecture des dernières valeurs par paire grâce à la clé de ligne `PAIRE#(9999999999999-ts)` (les plus récentes en premier) |
| **Streamlit + Altair** | interface | tableau de bord en Python, rafraîchi automatiquement |

Tables HBase (famille `d`) :

| Table | Contenu |
|---|---|
| `prix` | open, high, low, close, rendement de chaque bougie |
| `alertes` | bougies dont `|rendement|` dépasse le seuil |
| `volatilite` | écart-type glissant des rendements |

## 4. Résultats : interface

Le tableau de bord affiche, pour la paire choisie : le dernier prix, le rendement de la bougie en cours,
le nombre d'alertes, la courbe de prix, et le rendement de chaque bougie 1 minute face au seuil d'alerte (ligne rouge pointillée).

**ETHUSDT** : 16 alertes affichées, prix en hausse puis stabilisation.

![Dashboard ETHUSDT](<img width="1620" height="976" alt="Capture d&#39;écran 2026-11-09 231257" src="https://github.com/user-attachments/assets/92806bd0-55c8-4bfa-a000-4ccb7899244d" />
)

**BTCUSDT** : aucune alerte, le rendement reste sous le seuil (0,25 %).

![Dashboard BTCUSDT](<img width="1621" height="977" alt="Capture d&#39;écran 2026-11-09 231317" src="https://github.com/user-attachments/assets/3843d6bb-1f56-4bd9-85a5-a8465557bed6" />
)

## 5. Arborescence

```
crypto-bigdata/
├── README.md
├── requirements.txt            dépendances Python (poste hôte)
├── config.py                   paires, mois, adresses Kafka / HBase
├── docs/                       captures d'écran du dashboard
├── scripts/
│   ├── telecharger_historique.sh   télécharge les CSV Binance
│   ├── charger_hdfs.sh             copie les CSV sur HDFS
│   └── creer_tables_hbase.sh       crée les tables prix, alertes, volatilite
├── batch/
│   └── batch_seuils.py         Spark : volatilité + seuils -> seuils.json
├── streaming/
│   ├── producer.py             WebSocket Binance -> Kafka (mode simu inclus)
│   └── stream_job.py           Spark : Kafka -> rendement, alerte, volatilité -> HBase
├── storage/
│   ├── __init__.py
│   └── hbase_store.py          lecture / écriture HBase (happybase)
└── app/
    └── dashboard.py            interface Streamlit
```

## 6. Où chaque programme tourne

| Programme | Où |
|---|---|
| telecharger_historique.sh | conteneur (ou poste hôte) : il faut Internet, curl et unzip |
| charger_hdfs.sh, creer_tables_hbase.sh | conteneur `hadoop-master` |
| batch_seuils.py, stream_job.py | conteneur `hadoop-master` (spark-submit) |
| producer.py, dashboard.py | poste hôte (Python 3.8 ou plus) |

Les ports 9092 (Kafka) et 9090 (HBase Thrift) du conteneur doivent être publiés
vers le poste hôte (option `-p` de `docker run`).

## 7. Exécution

Prérequis : cluster Docker `hadoop-master` / `hadoop-slave1` / `hadoop-slave2` (image `liliasfaxi/spark-hadoop:hv-2.7.2`), projet copié dans le conteneur :
`docker cp . hadoop-master:/root/crypto-bigdata`.

### 7.1 Démarrer les services (conteneur)

```bash
cd /root && ./start-hadoop.sh
start-hbase.sh
hbase-daemon.sh start thrift
nohup /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties > /root/kafka.log 2>&1 &
```

Pour que le producteur du poste hôte puisse joindre Kafka, `server.properties` doit contenir :

```
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://127.0.0.1:9092
```

Créer le topic et les tables :

```bash
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 1 --topic klines
cd /root/crypto-bigdata && bash scripts/creer_tables_hbase.sh
```

### 7.2 Batch : historique et seuils (conteneur)

```bash
cd /root/crypto-bigdata
bash scripts/telecharger_historique.sh
bash scripts/charger_hdfs.sh
nohup spark-submit batch/batch_seuils.py > batch.log 2>&1 &
```

Résultat : `seuils.json` (un seuil par paire).

### 7.3 Temps réel : streaming (conteneur, Spark 2.4.8)

```bash
cd /root/crypto-bigdata
source /root/env24.sh
hdfs dfs -rm -r /crypto/chk
$SPARK_HOME/bin/spark-submit --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 streaming/stream_job.py
```

### 7.4 Producteur et interface (poste hôte)

```powershell
pip install -r requirements.txt
python streaming/producer.py          # données réelles Binance
python streaming/producer.py simu     # ou mode simulation (12 paires)
streamlit run app/dashboard.py
```

Vérification : `echo "count 'prix'" | hbase shell` doit afficher un nombre qui augmente.

## 8. Limites et pistes

- Seuil statique (quantile 99 % de l'historique) : il ne s'adapte pas aux changements de régime du marché.
- Une extension d'apprentissage automatique (prédire un mouvement brusque dans les 5 minutes) a été étudiée : GBT avec AUC-PR 0,42 contre 0,04 pour le hasard sur le jeu de test. Elle n'est pas incluse dans cette version.
