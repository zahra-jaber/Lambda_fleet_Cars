# Dataset intégré

Le fichier `vehicles.csv` est une copie de `exp1_14drivers_14cars_dailyRoutes.csv`.

- **14 véhicules** (`VEHICLE_ID`)
- Timestamp source conservé comme référence, mais réécrit à l'heure courante par le producteur pour simuler le temps réel
- Décimales avec virgule, pourcentages et unités nettoyés par `producer.py`
- `lat`, `lon`, `odometer_km`, `tire_pressure_psi` et `battery_v` sont absents du dataset et restent à `null`
- Le statut est calculé à partir de la température moteur et du niveau de carburant

Pour utiliser un autre CSV du même jeu, modifier `CSV_PATH` et `MAPPING_PATH` dans `docker-compose.yml`.
