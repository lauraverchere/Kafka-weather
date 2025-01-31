import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp, expr, to_json, struct, when, hour
)

# ---------------------------------------------------------------------------
# Étape 1 : Configuration de Spark
# ---------------------------------------------------------------------------
# TODO : Modifier "tp-esme" par le nom de votre répertoire GitHub ou chemin local

# Varible d'environnement qui permet d'indiquer à Python où se trouve Spark
os.environ["SPARK_HOME"] = "/workspaces/projet-esme/spark-3.2.3-bin-hadoop2.7"

# Création d'une session Spark
spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
# Étape 2 : Définition du schéma JSON
# ---------------------------------------------------------------------------

# Ce schéma est utilisé pour structurer les données JSON récupérées (par exemple, depuis une API ou un topic Kafka) 
# pour qu'elles puissent être manipulées facilement avec Spark.

weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("main", StringType())
    ]))),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("pressure", LongType()),
        StructField("humidity", LongType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ])),
    StructField("dt", LongType()),
    StructField("sys", StructType([
        StructField("country", StringType())
    ])),
    StructField("name", StringType())
])

# ---------------------------------------------------------------------------
# Étape 3 : Lecture des données en streaming depuis Kafka
# ---------------------------------------------------------------------------
# Initialisation d'un DataFrame de streaming Spark qui consomme les données provenant du topic-weather
df = (
    spark.readStream # Lecture des flux en temps réel
    .format("kafka") # Source de données Kakfa
    .option("kafka.bootstrap.servers", "localhost:9092") # Serveur Kafka
    .option("subscribe", "topic-weather") # Connexion au topic 
    .option("startingOffsets", "earliest") # A partir de quand Spark commence à lire les messages
    .option("failOnDataLoss", "false") # Ignorance des pertes de données et continue de lire les messages disponibles
    .load() # Chargement des données
)

# ---------------------------------------------------------------------------
# Étape 4 : Parsing et extraction des données
# ---------------------------------------------------------------------------
parsed_df = df.select(
    # Récupération des données message Kafka et conversion en string
    # On parse la chaîne de caractère selon le schéma weather_schema
    from_json(col("value").cast(StringType()), weather_schema).alias("data"),

    # Convertion de timestamp en long + Convertion en format standard (from_unixtime)
    # + Convertion en timestamp Spark
    to_timestamp(from_unixtime(col("timestamp").cast(LongType()))).alias("date")
)

processed_df = parsed_df.select(
    col("date"),
    col("data.name").alias("city"),
    col("data.sys.country").alias("pays"),
    col("data.coord.lon").alias("lon"),
    col("data.coord.lat").alias("lat"),
    expr("filter(data.weather, x -> x.main is not null)[0].main").alias("weather"),
    col("data.main.temp").alias("temperature"),
    col("data.main.pressure").alias("pressure"),
    col("data.main.humidity").alias("humidity"),
    col("data.wind.speed").alias("speed")
)

# ---------------------------------------------------------------------------
# Étape 5 : Création de nouvelles variables
# ---------------------------------------------------------------------------

# TODO : Ajouter une colonne "heat_index" avec la formule :

# température + (0.5555 * ((6.11 * (10 ^ ((7.5 * température) / (237.7 + température))) * (humidité / 100)) - 10))
processed_df = processed_df.withColumn(
    "heat_index",
    processed_df.temperature +
        (0.5555 * ((6.11 * ((10** ((7.5 * processed_df.temperature) / (237.7 + processed_df.temperature)))) * (processed_df.humidity / 100)) - 10))
)


# TODO : Ajouter une colonne "severity_index" avec la formule :
# (vitesse du vent * 0.5) + ((1015 - pression) * 0.3) + (humidité * 0.2)
processed_df = processed_df.withColumn(
    "severity_index",
    (processed_df.speed * 0.5) + ((1015 - processed_df.pressure) * 0.3) + (processed_df.humidity * 0.2)
)
 
 
# TODO : Ajouter une colonne "time_of_day" pour catégoriser la période de la journée :

# Matin (6h-12h), Après-midi (12h-18h), Soirée (18h-24h), Nuit (0h-6h)
processed_df = processed_df.withColumn(
    "time_of_day",
    when(hour(processed_df['date']) >= 6, 'Matin')
    .when(hour(processed_df['date']) >= 12, 'Après-midi')
    .when(hour(processed_df['date']) >= 18, 'Soirée')
    .otherwise('Nuit') 
)




# ---------------------------------------------------------------------------
# Étape 6 : Transformation des données en JSON pour Kafka
# ---------------------------------------------------------------------------
kafka_output_df = processed_df.select(
    to_json(struct(
        col("date"),
        col("city"),
        col("pays"),
        col("lon"),
        col("lat"),
        col("weather"),
        col("temperature"),
        col("pressure"),
        col("humidity"),
        col("speed")
    )).alias("value")  # Convertir les données en JSON pour Kafka
)

# ---------------------------------------------------------------------------
# Étape 7 : Écriture des résultats dans un topic Kafka
# ---------------------------------------------------------------------------
query = (kafka_output_df
    .writeStream # Activation de l'écriture des données vers un topic
    .format("kafka") # Vers un topic Kafka
    .outputMode("append")
    .option("kafka.bootstrap.servers", "localhost:9092")  # Adresse du broker Kafka
    .option("topic", "topic-weather-final")                   # Nom du topic Kafka de destination
    .option("checkpointLocation", "/tmp/checkpoints")    # Emplacement des checkpoints
    .start())

# Afficher les résultats dans la console (pour vérification)
console_query = (processed_df 
    .writeStream 
    .outputMode("append") # Ajoute chaque nouvelle ligne de données traitées à la console
    .format("console") # Destination : console
    .option("truncate", "false") # No tronquage des colonnes
    .start())

query.awaitTermination() # Attente que l'écriture dans le topic finisse
console_query.awaitTermination() # Attente que l'écriture dans la console se termine
