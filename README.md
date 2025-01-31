
# But et objectifs  
L’objectif principal est de fournir une application de monitoring météo en temps réel permettant aux utilisateurs de suivre les évolutions météorologiques à travers différentes régions et villes. Pour atteindre cet objectif, nous avons défini les axes suivants :  
  
- Collecter des données météorologiques en temps réel à partir de l'API OpenWeatherMap dans un topic Kafka nommé topic-weather.  
- Traiter ces données en temps réel à l’aide de Spark Streaming afin de générer de nouvelles variables, puis stocker les résultats dans un autre topic Kafka nommé topic-weather-final.
  
# Mise en œuvre et choix technologiques  
Pour répondre à ces besoins, Kafka est utilisé comme système de messagerie pour assurer une ingestion fluide des données en temps réel, tout en garantissant une haute disponibilité et une scalabilité horizontale. Spark Streaming est choisi pour son efficacité dans le traitement des flux de données en continu et sa capacité à s'intégrer facilement avec Kafka. Ce duo technologique permet d’assurer une faible latence et une transformation des données en temps réel, tout en restant robuste face aux charges importantes.  

# Etapes :  
1. API OpenWeatherMap  
   |  
   | --> (Récupèration des données météo + Envoie des données en temps réel au topic Kafka)  
   |  
2. Kafka (Topic 1: "topic-weather")  
   |  
   | --> (Collecte les données + Stockage des données météo brutes en continu)  
   |  
3. Spark Streaming  
   |  
   | --> (Lit les données de "topic-weather", les traite)  
   |  
4. Kafka (Topic 2: "topic-weather-final")  
   |  
   | --> (Stocke les données traitées, prêtes à être utilisées par l’application ou d’autres systèmes)  
   |  
5. Application utilisateur  
   |  
   | --> (Récupère et affiche les données météo traitées)  

