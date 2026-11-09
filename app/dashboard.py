# -*- coding: utf-8 -*-
# INTERFACE STREAMLIT : affiche en direct le prix, la volatilité et les alertes
# lus dans HBase.
#
# Lancement :  streamlit run app/dashboard.py

import os
import sys
import time

import altair as alt
import pandas as pd
import streamlit as st

# Permet d'importer config.py et le dossier storage/ de la racine du projet
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import config
from storage import hbase_store

# Titre et mise en page de la page
st.set_page_config(page_title="Crypto - Volatilité en direct", layout="wide")
st.title("Volatilité des cryptomonnaies en direct")

# Barre latérale : choix de la paire, du nombre de points et du rafraîchissement
pair = st.sidebar.selectbox("Paire", config.PAIRS)
n = st.sidebar.slider("Nombre de points affichés", 20, 500, 100)
auto = st.sidebar.checkbox("Rafraîchir toutes les 5 secondes", value=True)


# Lit une table HBase et la met en DataFrame trié du plus ancien au plus récent.
# errors="coerce" : une valeur illisible (par exemple "nan") devient une valeur
# manquante, puis la ligne est retirée.
def charger(table, colonnes_num):
    lignes = hbase_store.lire(table, pair, n)
    if not lignes:
        return pd.DataFrame()
    df = pd.DataFrame(lignes)
    df["heure"] = pd.to_datetime(df["ts_ms"], unit="ms")
    for c in colonnes_num:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=colonnes_num)
    return df.sort_values("heure").set_index("heure")


# Nombre de décimales adapté au niveau de prix (BTC : 2, LINK : 4, XRP et DOGE : 5)
def format_prix(valeur):
    if valeur >= 100:
        return ",.2f"
    if valeur >= 1:
        return ",.4f"
    return ",.5f"


# Couleur hexadécimale -> "rgba(r, g, b, opacité)" (pour le dégradé)
def rgba(hexa, opacite):
    h = hexa.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return "rgba({}, {}, {}, {})".format(r, g, b, opacite)


# Courbe au style "bourse" : ligne fine, zone dégradée qui s'estompe vers le bas,
# graduations sobres (76.00K), ligne pointillée au dernier niveau avec son prix.
# L'échelle est ajustée aux données (le zéro n'est pas forcé).
#   serie   : valeurs indexées par l'heure
#   fmt     : format des valeurs (",.2f" prix, ".3%" pourcentage)
#   remplir : zone dégradée sous la courbe + dernier niveau en pointillés
#   lignes  : lignes horizontales à tracer, liste de (valeur, couleur)
def courbe(serie, couleur="#F0B90B", fmt=",.2f", remplir=True, lignes=()):
    d = serie.dropna().reset_index()
    d.columns = ["heure", "valeur"]
    if d.empty:
        return None

    # Bornes de l'axe vertical : min et max des données (et des lignes), + 8 %
    valeurs = d["valeur"].tolist() + [v for v, _ in lignes]
    bas, haut = min(valeurs), max(valeurs)
    ecart = haut - bas
    marge = ecart * 0.08 if ecart > 0 else (abs(haut) * 0.001 or 1e-9)
    bas, haut = bas - marge, haut + marge
    echelle = alt.Scale(domain=[bas, haut], zero=False, nice=False)
    d["bas"] = bas

    # Grands nombres affichés en milliers : 76.00K ; sinon le format demandé
    en_milliers = max(abs(bas), abs(haut)) >= 1000 and "%" not in fmt
    options_y = {}
    if en_milliers:
        options_y["labelExpr"] = "format(datum.value / 1000, ',.2f') + 'K'"

    # Petite marge à droite pour écrire le dernier prix
    t0, t1 = d["heure"].iloc[0], d["heure"].iloc[-1]
    duree = (t1 - t0) if t1 > t0 else pd.Timedelta(seconds=10)
    fin_axe = t1 + duree * 0.10
    echelle_x = alt.Scale(domain=[t0.isoformat(), fin_axe.isoformat()])

    gris = "#9ca3af"
    # Mêmes axes pour toutes les couches : une couche sans axe ferait
    # disparaître l'axe de tout le graphique
    axis_x = alt.Axis(format="%H:%M", tickCount=6, grid=False, ticks=False,
                      labelColor=gris, domainColor="#374151")
    axis_y = alt.Axis(format=fmt, tickCount=8, grid=False, ticks=False,
                      domain=False, labelColor=gris, labelFontSize=12,
                      orient="left", **options_y)
    axe_x = alt.X("heure:T", title=None, scale=echelle_x, axis=axis_x)
    axe_y = alt.Y("valeur:Q", title=None, scale=echelle, axis=axis_y)
    base = alt.Chart(d).encode(x=axe_x, y=axe_y)

    couches = []
    if remplir:
        # Dégradé vertical : couleur en haut, transparent en bas
        degrade = alt.Gradient(
            gradient="linear", x1=1, x2=1, y1=1, y2=0,
            stops=[alt.GradientStop(color=rgba(couleur, 0.02), offset=0),
                   alt.GradientStop(color=rgba(couleur, 0.45), offset=1)])
        couches.append(base.mark_area(color=degrade, line=False).encode(y2="bas:Q"))
    couches.append(base.mark_line(color=couleur, strokeWidth=2))

    # Lignes horizontales (par exemple les seuils d'alerte)
    for valeur, c in lignes:
        regle = alt.Chart(pd.DataFrame({"v": [valeur]}))
        couches.append(regle.mark_rule(color=c, strokeDash=[4, 4], strokeWidth=1.5)
                       .encode(y=alt.Y("v:Q", title=None, scale=echelle, axis=axis_y)))

    # Dernier niveau : ligne pointillée sur toute la largeur + valeur écrite à droite
    if remplir:
        v = float(d["valeur"].iloc[-1])
        texte = "{:,.2f}K".format(v / 1000) if en_milliers else format(v, fmt)
        couches.append(alt.Chart(pd.DataFrame({"v": [v]}))
                       .mark_rule(color=gris, strokeDash=[2, 3])
                       .encode(y=alt.Y("v:Q", title=None, scale=echelle, axis=axis_y)))
        couches.append(alt.Chart(pd.DataFrame({"v": [v], "t": [fin_axe], "txt": [texte]}))
                       .mark_text(align="right", baseline="bottom", dy=-4, fontSize=12,
                                  fontWeight="bold", color=couleur)
                       .encode(x=alt.X("t:T", title=None, scale=echelle_x, axis=axis_x),
                               y=alt.Y("v:Q", title=None, scale=echelle, axis=axis_y), text="txt:N"))

    return (alt.layer(*couches).properties(height=320)
            .configure_view(stroke=None))


# Affiche un graphique Altair sur toute la largeur de la page
def afficher(graphique):
    if graphique is None:
        st.write("Pas de données.")
        return
    try:
        st.altair_chart(graphique, width="stretch")
    except TypeError:
        # versions plus anciennes de Streamlit
        st.altair_chart(graphique, use_container_width=True)


# Lecture des trois tables ; si HBase est injoignable on affiche l'erreur
try:
    prix = charger("prix", ["close", "ret", "seuil"])
    vol = charger("volatilite", ["vol"])
    alertes = charger("alertes", ["close", "ret", "seuil"])
except Exception as e:
    st.error("Impossible de lire HBase : {}".format(e))
    st.info("Vérifier que HBase et son serveur Thrift tournent (port {}).".format(config.HBASE_PORT))
    st.stop()

# Bandeau de chiffres clés
if prix.empty:
    st.warning("Pas encore de données pour {}. Le job streaming tourne-t-il ?".format(pair))
else:
    dernier = prix.iloc[-1]
    fmt = format_prix(dernier["close"])
    c1, c2, c3 = st.columns(3)
    c1.metric("Dernier prix", format(dernier["close"], fmt))
    c2.metric("Rendement (bougie en cours)", "{:+.2%}".format(dernier["ret"]))
    c3.metric("Alertes affichées", len(alertes))

    # Courbe du prix, avec une échelle centrée sur les valeurs
    st.subheader("Prix")
    afficher(courbe(prix["close"], fmt=fmt))

    # Courbe du rendement avec le seuil d'alerte (haut et bas) en rouge pointillé
    st.subheader("Rendement de la bougie 1 minute et seuil d'alerte")
    seuil = float(dernier["seuil"])
    afficher(courbe(prix["ret"], couleur="#3B82F6", fmt=".3%", remplir=False,
                    lignes=[(seuil, "#EF4444"), (-seuil, "#EF4444")]))

# Courbe de la volatilité sur 5 minutes
st.subheader("Volatilité sur 5 minutes")
if vol.empty:
    st.write("La volatilité apparaît après quelques minutes de données.")
else:
    afficher(courbe(vol["vol"], couleur="#A855F7", fmt=".3%"))

# Tableau des dernières alertes (la plus récente en haut)
st.subheader("Dernières alertes")
if alertes.empty:
    st.write("Aucune alerte pour le moment.")
else:
    st.dataframe(alertes[["close", "ret", "seuil"]].sort_index(ascending=False))

# Rafraîchissement automatique : attend 5 secondes puis relance la page
if auto:
    time.sleep(5)
    st.rerun()