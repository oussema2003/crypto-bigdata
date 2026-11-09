# Volatilité des cryptomonnaies : batch + temps réel

- **Batch :** historique Binance sur HDFS -> volatilité et seuil d'alerte (Spark SQL).
- **Temps réel :** WebSocket Binance -> Kafka -> Spark -> HBase -> interface Streamlit.

Définition du rendement (identique dans le batch et le streaming) :
`rendement d'une bougie 1 minute = (close - open) / open`

## Arborescence

```
crypto-bigdata/
├── README.md
├── requirements.txt            dépendances Python (poste hôte)
├── config.py                   paires, mois, adresses Kafka / HBase
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

## Où chaque programme tourne

| Programme | Où |
|---|---|
| telecharger_historique.sh | conteneur (ou poste hôte) : il faut Internet, curl et unzip |
| charger_hdfs.sh, creer_tables_hbase.sh | conteneur `hadoop-master` |
| batch_seuils.py, stream_job.py | conteneur `hadoop-master` (spark-submit) |
| producer.py, dashboard.py | poste hôte (Python 3.8 ou plus) |

Les ports 9092 (Kafka) et 9090 (HBase Thrift) du conteneur doivent être publiés
vers le poste hôte (option `-p` de `docker run`).

