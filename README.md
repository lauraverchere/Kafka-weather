
# But et objectifs  
L’objectif principal est de fournir une application de monitoring météo en temps réel permettant aux utilisateurs de suivre les évolutions météorologiques à travers différentes régions et villes. Pour atteindre cet objectif, nous avons défini les axes suivants :  
  
- Collecter des données météorologiques en temps réel à partir de l'API OpenWeatherMap dans un topic Kafka nommé topic-weather.  
- Traiter ces données en temps réel à l’aide de Spark Streaming afin de générer de nouvelles variables, puis stocker les résultats dans un autre topic Kafka nommé topic-weather-final.
  
# Mise en œuvre et choix technologiques  
Pour répondre à ces besoins, Kafka est utilisé comme système de messagerie pour assurer une ingestion fluide des données en temps réel, tout en garantissant une haute disponibilité et une scalabilité horizontale. Spark Streaming est choisi pour son efficacité dans le traitement des flux de données en continu et sa capacité à s'intégrer facilement avec Kafka. Ce duo technologique permet d’assurer une faible latence et une transformation des données en temps réel, tout en restant robuste face aux charges importantes.  

# Etapes :  

1. API OpenWeatherMap  
   |  
   | --> **Producer**  
   | &nbsp;    (envoie les données météo brutes en continu)  
   |
2. Kafka (Topic 1: "topic-weather")  
   |  
   | --> **Consumer** (Spark Streaming)   
   |     (lit les données de "topic-weather", les traite en temps réel :  
   |      calculs, transformations, enrichissements, etc.)  
   |  
   | --> **Producer** (Spark Streaming)  
   |     (écrit les données traitées dans "topic-weather-final")  
   |  
3. Kafka (Topic 2: "topic-weather-final")  
   |  
   | --> **Consumer** (Application utilisateur)  
   |     (récupère les données finales pour les afficher à l'utilisateur)
