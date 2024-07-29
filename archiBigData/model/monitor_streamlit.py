import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pymongo import MongoClient

# Créer une session Spark
spark = SparkSession.builder \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .getOrCreate()

# Charger les données des résultats depuis MongoDB avec Spark
df_results = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://db:27017/olympic_game.olympic_results") \
    .load()

# Compter le nombre total de médailles attribuées pour chaque événement
medals_count_per_event = df_results.groupBy("slug_game").count().orderBy("count", ascending=False)

# Sélectionner les événements où il y a eu le plus de médailles distribuées
top_medals_events = medals_count_per_event.limit(10).toPandas()

# Affichage des événements avec le plus de médailles distribuées
st.write("Top 10 des événements avec le plus de médailles distribuées :")
st.write(top_medals_events)

# Création du graphique
plt.figure(figsize=(10, 6))
sns.barplot(x='count', y='slug_game', data=top_medals_events)
plt.xlabel("Nombre de médailles")
plt.ylabel("Événement")
plt.title("Top 10 des événements avec le plus de médailles distribuées")
plt.tight_layout()

# Affichage du graphique dans Streamlit
st.pyplot(plt)
