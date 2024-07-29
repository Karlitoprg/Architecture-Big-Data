from pyspark.sql import SparkSession
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Créer une session Spark
spark = SparkSession.builder.getOrCreate()

# Connexion à la base de données MongoDB
client = MongoClient("mongodb://db:27017/")
db = client.olympic_game
collection = db.athletes

# Charger les données depuis MongoDB avec Spark
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://db:27017/olympic_game.athletes") \
    .load()

# Exemple de création d'un graphique avec Matplotlib
# Ici, nous allons simplement créer un histogramme du nombre d'athlètes par pays
athletes_per_country = df.groupBy("country_name").count().orderBy("count", ascending=False).toPandas()

plt.figure(figsize=(10, 6))
plt.bar(athletes_per_country["country_name"], athletes_per_country["count"])
plt.xlabel("Pays")
plt.ylabel("Nombre d'athlètes")
plt.title("Nombre d'athlètes par pays")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()