# Airflow × PySpark — Pipeline de traitement de logs

## 📌 Description

Ce projet met en place un pipeline automatisé avec **Apache Airflow** et **PySpark** pour traiter des fichiers de logs. Le pipeline :

* Lit un fichier brut non trié au format `timestamp @url`
* Extrait le **Breakdown\_Type** (nombre après `slab/`)
* Extrait le **Breakdown\_Level** (nombre après `256`)
* Trie par timestamp et, en cas d'égalité, par Breakdown\_Type
* Sauvegarde les données propres dans `output/` et les lignes invalides dans `bad_lines/`

## 📂 Structure du projet

```
├── dags/
│   └── breakdown_dag.py          # Définition du DAG Airflow
├── jobs/
│   └── breakdown_job.py          # Script PySpark pour le traitement
├── data/                         # Données d'entrée (logs bruts)
├── output/                       # Résultats CSV générés
├── bad_lines/                    # Lignes invalides
├── docker-compose.yml            # Configuration des conteneurs Airflow
├── Dockerfile                    # Image personnalisée (Java 17 + PySpark)
├── requirements.txt              # Dépendances Python
└── wheels/                       # Fichiers wheel pour installation offline
```

## 📦 Rôle des fichiers **wheels**

Les **fichiers wheel (.whl)** sont des paquets Python précompilés. Dans ce projet, ils permettent :

* **Installation offline** : PySpark et ses dépendances peuvent être installés même sans connexion Internet, ce qui est utile dans un environnement restreint.
* **Gain de temps** : l’installation depuis des wheels est plus rapide que depuis le code source.
* **Reproductibilité** : on utilise exactement la même version des bibliothèques, évitant des problèmes de compatibilité.

Ici, le Dockerfile copie ces wheels dans le conteneur et les installe localement :

```dockerfile
COPY wheels/ /wheels/
RUN pip install --no-cache-dir /wheels/py4j-*.whl && \
    pip install --no-cache-dir /wheels/pyspark-*-py2.py3-none-any.whl
```

Cela garantit que le pipeline fonctionne même dans un environnement sans accès Internet.

## ⚙️ Prérequis

* **Docker** & **Docker Compose**
* Port **8089** libre pour l’interface Airflow
* Python 3.11 (si exécution locale des scripts)

## 🚀 Installation & Lancement

1. Cloner le dépôt :

```bash
git clone https://github.com/Cherif-D/airflow-pyspark-pipeline.git
cd airflow-pyspark-pipeline
```

2. Initialiser Airflow :

```bash
docker compose up airflow-init
```

3. Démarrer les services :

```bash
docker compose up -d
```

4. Accéder à l’interface : [http://localhost:8089](http://localhost:8089)

## 📜 Utilisation du DAG

* **Nom du DAG** : `breakdown_pipeline`
* Étapes :

  1. `validate_input` → Vérifie la présence du fichier source
  2. `fix_pyspark_perms` → Crée et autorise les répertoires nécessaires
  3. `Read` → Lecture brute & identification des lignes invalides
  4. `SortData` → Tri des données
  5. `Compute` → Extraction des valeurs Breakdown et calculs
  6. `SaveResult` → Sauvegarde des CSV

Déclenchement manuel : via UI ou :

```bash
airflow dags trigger breakdown_pipeline
```

## 🛠 Commandes utiles

* Pause du DAG :

```bash
airflow dags pause breakdown_pipeline
```

* Reprise :

```bash
airflow dags unpause breakdown_pipeline
```

* Consultation des DAGs :

```bash
airflow dags list
```

## 📌 Notes

* Aucun paramétrage RAM/CPU n’est défini dans le dépôt → ajuster si besoin dans `docker-compose.yml` ou le code Spark.
* Les wheels sont essentiels si le déploiement se fait dans un environnement sans accès Internet.
