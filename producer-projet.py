from kafka import KafkaProducer
import requests
import json
import time

# ---------------------------------------------------------------------------
# Étape 1 : Configuration
# ---------------------------------------------------------------------------
API_KEY = '60c351069138b06a8e6cc9b06d8c4752'

CITIES = ['Paris', 'London', 'Tokyo', 'Kuala Lumpur', 'Annecy']  # Exemple : Ajoutez ici vos villes

KAFKA_TOPIC = 'topic-weather'

# Adresse du serveur Kafka
KAFKA_SERVER = 'localhost:9092'

# ---------------------------------------------------------------------------
# Étape 2 : Initialisation du producteur Kafka
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation en JSON
)

# ---------------------------------------------------------------------------
# Étape 3 : Fonction pour récupérer les données météo
# ---------------------------------------------------------------------------
def get_weather_data(city):
    """
    Fonction qui récupère les données météo d'une ville depuis l'API OpenWeather.
    :param city: Nom de la ville
    :return: Données JSON de la météo
    """
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

# ---------------------------------------------------------------------------
# Étape 4 : Envoi des données en continu vers Kafka
# ---------------------------------------------------------------------------
while True:
    for city in CITIES:
        data = get_weather_data(city)  # Récupération des données météo
        if data:
            producer.send(KAFKA_TOPIC, key=city.encode('utf-8'), value=data)  # Envoi au topic Kafka
            print(f"Données envoyées pour {city}: {data}")
    time.sleep(60)  # Pause d'une minute entre chaque envoi
