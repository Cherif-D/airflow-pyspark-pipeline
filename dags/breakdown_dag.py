# dags/breakdown_dag.py
"""
DAG Airflow pour orchestrer le pipeline de traitement de logs TIMESTAMP@URL.

Chaîne d'exécution (task dependency graph) :

    validate_input -> fix_pyspark_perms -> Read -> SortData -> Compute -> SaveResult

Résumé des tâches :
- validate_input : vérifie la présence du fichier d'entrée (INPUT).
- fix_pyspark_perms : s'assure que les scripts PySpark sont exécutables et que les dossiers existent.
- Read : lance le script PySpark en mode étape READ (parse + split good/bad).
- SortData : lance l'étape de tri global (timestamp, arrival_id).
- Compute : calcule les runs et le résumé.
- SaveResult : produit les CSV finaux (final.csv, exo_output.csv, runs.csv, exo_runs_output.csv).

IMPORTANT :
- Le code fonctionnel d'origine est strictement conservé. Seuls des commentaires explicatifs
  (docstring + commentaires Python) ont été ajoutés pour faciliter la lecture.
- Les chemins sont pensés pour un conteneur Airflow (ex: images officielles ou docker-compose).
"""

# -----------------------------
# Imports
# -----------------------------
from datetime import datetime, timedelta  # Pour la planification (start_date, retry_delay)
import os                                 # Pour vérifier l'existence des fichiers/chemins
from airflow import DAG                   # Classe principale d'un DAG Airflow
from airflow.operators.python import PythonOperator  # Opérateur pour exécuter du Python natif
from airflow.operators.bash import BashOperator      # Opérateur pour lancer des commandes shell

# -----------------------------
# Constantes de chemin (dans le conteneur Airflow)
# -----------------------------
INPUT  = "/opt/airflow/data/break_down.txt"  # Fichier brut à traiter
OUTPUT = "/opt/airflow/output"               # Dossier de sortie (CSV + parquets en mode tout-en-un)
BAD    = "/opt/airflow/bad_lines"            # Dossier où stocker les lignes rejetées (bad lines)
STAGE  = "/opt/airflow/stage"                # Dossier intermédiaire pour les étapes (read/sort/compute/save)
JOB    = "/opt/airflow/jobs/breakdown_job.py"# Script PySpark/Python qui exécute le pipeline
FINAL  = f"{OUTPUT}/final.csv"                # Chemin du CSV final (résumé par type)

# -----------------------------
# Paramètres par défaut des tâches (appliqués à toutes les tasks du DAG)
# -----------------------------
# - owner : identifiant du propriétaire logique du DAG
# - depends_on_past : False => chaque run est indépendant des runs passés
# - retries / retry_delay : logique de relance en cas d'échec
# NOTE : Airflow gère l'horodatage en UTC; start_date est interprétée en UTC.
default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# -----------------------------
# Fonction utilitaire : valider la présence du fichier d'entrée
# -----------------------------
# Cette fonction est appelée par un PythonOperator. En cas d'absence,
# on lève une exception pour stopper le DAG dès le début.

def validate_input():
    """Vérifie que le fichier INPUT existe; sinon lève FileNotFoundError."""
    if not os.path.exists(INPUT):
        raise FileNotFoundError(f"Fichier manquant : {INPUT}")

# -----------------------------
# Définition du DAG et des tâches
# -----------------------------
# - dag_id : nom unique du DAG dans Airflow
# - description : brève description affichée dans l'UI
# - default_args : dict défini ci-dessus
# - start_date : date de début (doit être dans le passé pour permettre un run manuel)
# - schedule : None => pas de planification automatique (déclenchement manuel)
# - catchup : False => pas de rattrapage des exécutions passées
# - tags : étiquettes pour filtrage dans l'UI
with DAG(
    dag_id="breakdown_pipeline",
    description="Read -> SortData -> Compute -> SaveResult",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,   # déclenchement manuel
    catchup=False,
    tags=["pyspark","logs","breakdown"],
) as dag:

    # -------------------------
    # 1) Validation des prérequis
    # -------------------------
    # Exécute la fonction validate_input ci-dessus. Si le fichier INPUT est absent,
    # la tâche échoue et tout le DAG s'arrête (les tâches en aval ne s'exécutent pas).
    t_validate = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
    )

    # -------------------------
    # 2) Correctifs de permissions + création des dossiers
    # -------------------------
    # Cette tâche :
    # - rend exécutables les scripts dans pyspark/bin, suivant différentes locations possibles
    #   selon la distribution Python dans le conteneur
    # - crée les dossiers STAGE / OUTPUT / BAD
    # - liste STAGE pour debug
    # NOTE : on garde la commande bash EXACTE, sans ajouter de commentaires internes
    #        pour ne pas altérer la chaîne.
    t_fix_perms = BashOperator(
        task_id="fix_pyspark_perms",
        bash_command=f"""
set -e
for d in \
  ~/.local/lib/python*/site-packages/pyspark/bin \
  /usr/local/lib/python*/dist-packages/pyspark/bin \
  /usr/local/lib/python*/site-packages/pyspark/bin
do
  [ -d "$d" ] && chmod +x "$d"/* || true
done
mkdir -p {STAGE} {OUTPUT} {BAD}
ls -la {STAGE} || true
""",
        do_xcom_push=False,
    )

    # -------------------------
    # 3) Variables d'environnement pour les tâches Bash
    # -------------------------
    # - PYSPARK_PYTHON : interpréteur Python utilisé côté exécutant (ici "python")
    # - JAVA_HOME      : JDK à utiliser (important pour PySpark)
    envs = {"PYSPARK_PYTHON": "python", "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"}

    # -------------------------
    # 4) Étape READ (parse + split good/bad) via breakdown_job.py
    # -------------------------
    # On appelle le script avec --stage read et les bons chemins. do_xcom_push=False
    # car on n'a pas besoin de renvoyer la sortie dans XCom (on travaille sur fichiers/parquets).
    t_read = BashOperator(
        task_id="Read",
        bash_command=f"python {JOB} --stage read --input {INPUT} --stage_dir {STAGE} --bad {BAD} --output {OUTPUT}",
        env=envs,
        do_xcom_push=False,
    )

    # -------------------------
    # 5) Étape SORT (tri global)
    # -------------------------
    t_sort = BashOperator(
        task_id="SortData",
        bash_command=f"python {JOB} --stage sort --stage_dir {STAGE} --input {INPUT} --output {OUTPUT} --bad {BAD}",
        env=envs,
        do_xcom_push=False,
    )

    # -------------------------
    # 6) Étape COMPUTE (calcul des runs + summary)
    # -------------------------
    t_compute = BashOperator(
        task_id="Compute",
        bash_command=f"python {JOB} --stage compute --stage_dir {STAGE} --input {INPUT} --output {OUTPUT} --bad {BAD}",
        env=envs,
        do_xcom_push=False,
    )

    # -------------------------
    # 7) Étape SAVE (génération des CSV finaux)
    # -------------------------
    t_save = BashOperator(
        task_id="SaveResult",
        bash_command=f"python {JOB} --stage save --stage_dir {STAGE} --final_csv {FINAL} --input {INPUT} --output {OUTPUT} --bad {BAD}",
        env=envs,
        do_xcom_push=False,
    )

    # -------------------------
    # Chaînage/ordonnancement des tâches (>> signifie "doit s'exécuter avant")
    # -------------------------
    t_validate >> t_fix_perms >> t_read >> t_sort >> t_compute >> t_save
