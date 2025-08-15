# jobs/breakdown_job.py
"""
Traitement des logs TIMESTAMP@URL (ex: 2017-10-05 00:01:09@/map/1.0/slab/standard/256/19/...)
Deux modes :
- Tout-en-un (sans --stage)
- Par étapes (--stage read|sort|compute|save + --stage_dir)

Sorties :
- /output/final.csv           (synthèse complète par type)
- /output/exo_output.csv      (résumé minimal par type)
- /output/exo_runs_output.csv (FORMAT EXERCICE : une ligne par run global)
- /output/runs.csv            (détail des runs dédoublonnés)

NOTES IMPORTANTES (commentaires pédagogiques) :
- Le pipeline lit des lignes brutes "timestamp@url", parse les champs utiles, filtre les lignes valides,
  calcule des résumés par type de panne (breakdown_type) et des "runs" globaux dans la timeline.
- Deux manières de lancer :
    1) Mode tout-en-un (plus simple) : on donne --input, --output, --bad, et tout est fait dans la foulée.
    2) Mode par étapes (plus proche d'un DAG) : --stage read / sort / compute / save, avec un --stage_dir pour persister entre étapes.
- Le code ci-dessous est intact fonctionnellement : seuls des commentaires explicatifs ont été ajoutés.
"""

# -----------------------------
# Imports
# -----------------------------
import argparse               # Parse les arguments de la ligne de commande
import os, time, glob, shutil # Gestion des chemins, du temps, du listing de fichiers et des copies/déplacements
from pyspark.sql import SparkSession      # Pour créer une session Spark
from pyspark.sql import functions as F     # Fonctions SQL (col, split, when, regexp_extract, etc.)
from pyspark.sql.window import Window      # Fenêtres pour calculs analytiques (lag, somme cumulée, etc.)

# ----------------------------------------------------------
# 0) Session Spark
# ----------------------------------------------------------
def spark_session(app="breakdown_pipeline"):
    """
    Crée et retourne une SparkSession avec le nom d'application fourni.
    - app: nom de l'application (utile pour l'UI Spark et les logs)
    """
    return SparkSession.builder.appName(app).getOrCreate()  # getOrCreate évite de recréer si déjà ouverte

# ----------------------------------------------------------
# Utilitaire : écrire un CSV unique (coalesce(1) + renommage)
# ----------------------------------------------------------
def write_single_csv(df, out_path):
    """
    Écrit un DataFrame Spark en un seul fichier CSV à l'emplacement exact out_path.
    Détails :
    - Spark écrit d'office un dossier avec plusieurs part-*; on force coalesce(1) pour une seule part.
    - On écrit dans un dossier temporaire, puis on déplace le fichier part-*.csv vers out_path.
    - On nettoie le dossier temporaire.
    """
    out_dir = os.path.dirname(out_path)                                  # Dossier cible
    tmp_dir = os.path.join(out_dir, f"tmp_{os.path.basename(out_path)}") # Dossier temporaire unique
    if os.path.exists(tmp_dir):                                          # Si déjà présent, on supprime pour repartir propre
        shutil.rmtree(tmp_dir)
    os.makedirs(out_dir, exist_ok=True)                                  # Crée le dossier de sortie si nécessaire
    df.coalesce(1).write.mode("overwrite").csv(tmp_dir, header=True)    # Écrit une seule part CSV dans le tmp
    part = next(p for p in glob.glob(os.path.join(tmp_dir, "part-*.csv")))  # Récupère le fichier part-*.csv écrit par Spark
    if os.path.exists(out_path):                                         # Si un ancien fichier existe, on le supprime
        os.remove(out_path)
    shutil.move(part, out_path)                                          # Déplace/renomme la part en fichier final voulu
    shutil.rmtree(tmp_dir)                                               # Nettoyage du répertoire temporaire

# ----------------------------------------------------------
# 1) Lecture brute
# ----------------------------------------------------------
def read_raw(spark, input_path):
    """
    Lit le fichier brut en une seule colonne 'value' puis sépare en 'ts_str' et 'url' sur le séparateur '@'.
    - spark: SparkSession
    - input_path: chemin du fichier brut (ex: /opt/airflow/data/break_down.txt)
    Retour: DataFrame avec colonnes [ts_str, url, value]
    """
    df = spark.read.text(input_path)                    # Lecture brute : une colonne 'value' contenant toute la ligne
    split_col = F.split(F.col("value"), "@", 2)        # Split en [timestamp_str, url] avec au plus 2 éléments
    return df.select(
        F.trim(split_col.getItem(0)).alias("ts_str"),   # Chaîne de timestamp brut
        F.trim(split_col.getItem(1)).alias("url"),      # Chaîne d'URL brute
        F.col("value")                                  # Ligne originale conservée pour traçabilité
    )

# ----------------------------------------------------------
# 2) Parsing champs
# ----------------------------------------------------------
def parse_fields(df):
    """
    Parse les champs structurés à partir de 'ts_str' et 'url'.
    - Convertit ts_str -> timestamp (Spark TimestampType) via format yyyy-MM-dd HH:mm:ss
    - Extrait breakdown_type depuis /slab/<type>/ ou /multi-descr/<type>/ (priorité à slab)
    - Extrait breakdown_level depuis le motif '/256/<level>/'
    - Cast breakdown_level en int pour trier/agréger numériquement
    - Ajoute un 'arrival_id' monotone pour stabiliser l'ordre en cas d'égalité de timestamps
    """
    df = df.withColumn("timestamp", F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"))  # Conversion de la chaîne vers Timestamp

    type_from_slab = F.regexp_extract("url", r"/slab/([^/]+)/", 1)          # Capture le segment entre /slab/ et le / suivant
    type_from_multi = F.regexp_extract("url", r"/multi-descr/([^/]+)/", 1)   # Capture le segment entre /multi-descr/ et le / suivant

    df = df.withColumn(
        "breakdown_type",
        F.when(type_from_slab != "", type_from_slab)                          # Si trouvé dans slab -> utilise-le
         .when((type_from_slab == "") & (type_from_multi != ""), type_from_multi)  # Sinon si trouvé dans multi-descr
         .otherwise(F.lit(None))                                               # Sinon NULL
    )

    df = df.withColumn("breakdown_level", F.regexp_extract("url", r"/256/([^/]+)/", 1))  # Extrait le niveau après /256/
    df = df.withColumn("breakdown_level_int", F.col("breakdown_level").cast("int"))      # Version entière pour tri/agrégations
    df = df.withColumn("arrival_id", F.monotonically_increasing_id())                     # Id monotone pour ordre stable
    return df

# ----------------------------------------------------------
# 3) Split good/bad
# ----------------------------------------------------------
def split_good_bad(df):
    """
    Sépare les lignes "bonnes" (toutes les infos présentes) des lignes "bad" (incomplètes / non parseables).
    - good: timestamp non nul, breakdown_type non vide, breakdown_level non vide
    - bad: tout le reste (via différence d'ensembles)
    Retour: (good, bad)
    """
    good = (
        df.filter(
            (F.col("timestamp").isNotNull()) &                                  # timestamp valide
            (F.col("breakdown_type").isNotNull()) & (F.col("breakdown_type") != "") &  # type présent et non vide
            (F.col("breakdown_level").isNotNull()) & (F.col("breakdown_level") != "")  # niveau présent et non vide
        )
        .select("timestamp","arrival_id","breakdown_type",
                "breakdown_level","breakdown_level_int","url","value")   # Colonnes utiles pour la suite
    )
    bad = df.select("value").subtract(good.select("value"))                   # Les mauvaises lignes = tout - good
    return good, bad

# ----------------------------------------------------------
# 4) Runs par type + résumé
# ----------------------------------------------------------
def compute_runs_and_summary_partitioned(good):
    """
    Calcule (1) un ordre des événements et (2) un résumé par breakdown_type, avec un détail des "runs" par type.

    IMPORTANT: Dans cette implémentation, on marque un nouveau run au SEUL premier événement de chaque type
    (prev_ts est NULL uniquement pour la première ligne du type), donc chaque breakdown_type n'a qu'UN run_id.
    Par conséquent, pour un type donné :
      - run_length = nombre total d'événements de ce type
      - runs_count = 1
      - max_run_length = run_length
    Ce comportement correspond strictement au code fourni; il est documenté ici sans changement.
    """
    # Tri par type puis par (timestamp, arrival_id) pour un ordre reproductible
    ordered = good.orderBy("breakdown_type", "timestamp", "arrival_id")

    # Fenêtre partitionnée par type (ordre chronologique)
    w = Window.partitionBy("breakdown_type").orderBy("timestamp", "arrival_id")
    ordered = ordered.withColumn("prev_ts", F.lag("timestamp").over(w))         # Timestamp précédent au sein du type
    ordered = ordered.withColumn("is_new_run",                                   # 1 si première ligne du type, 0 sinon
                                 F.when(F.col("prev_ts").isNull(), 1).otherwise(0))
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)          # Fenêtre cumulative depuis le début du type
    ordered = ordered.withColumn("run_id", F.sum("is_new_run").over(w_cum))    # Somme cumulée -> run_id constant (=1) par type

    # Détail par run (ici 1 run par type) avec niveaux distincts triés
    runs = (
        ordered.groupBy("breakdown_type","run_id").agg(
            F.count("*").alias("run_length"),                                  # Nombre d'événements dans le run
            F.min("timestamp").alias("run_start"),                            # Début du run
            F.max("timestamp").alias("run_end"),                              # Fin du run
            F.array_sort(F.collect_set("breakdown_level_int")).alias("levels_in_run_int")  # Niveaux distincts triés
        )
        .withColumn(
            "levels_in_run_str",
            F.expr("array_join(transform(levels_in_run_int, x -> cast(x as string)), ',')") # Liste CSV des niveaux
        )
        .drop("levels_in_run_int")
    )

    # Résumé par type (agrégats à partir des runs)
    summary_core = runs.groupBy("breakdown_type").agg(
        F.sum("run_length").cast("long").alias("total_events"),   # Total d'événements pour ce type
        F.count("run_id").alias("runs_count"),                      # Nombre de runs (ici 1)
        F.max("run_length").alias("max_run_length"),                # Longueur max (ici = run_length)
        F.round(F.avg("run_length"), 2).alias("avg_run_length")     # Moyenne des longueurs (ici = run_length)
    )

    # Niveaux distincts globaux par type (à partir de toutes les lignes)
    levels_global = (
        ordered.groupBy("breakdown_type").agg(
            F.array_sort(F.collect_set("breakdown_level_int")).alias("distinct_levels_int")
        )
        .withColumn(
            "distinct_levels_str",
            F.expr("array_join(transform(distinct_levels_int, x -> cast(x as string)), ',')")
        )
        .drop("distinct_levels_int")
    )

    # Jointure du résumé principal avec la liste des niveaux distincts
    summary = summary_core.join(levels_global, on="breakdown_type", how="left")
    return ordered, runs, summary

# ----------------------------------------------------------
# 4-bis) Runs GLOBAUX (format énoncé)
# ----------------------------------------------------------
def compute_global_runs_for_exo(ordered_global):
    """
    Calcule les "runs globaux" dans la timeline complète : un run commence à chaque fois que le breakdown_type
    change (ou au tout début). C'est le format attendu par l'exercice (une ligne par run successif global).

    Entrée: ordered_global doit être trié par (timestamp, arrival_id) à l'échelle GLOBALE.
    Sortie colonnes :
      - Breakdown_Type       : le type sur ce run
      - Count_Successive     : nombre d'événements consécutifs de ce type dans ce run global
      - Breakdowns_Level     : les niveaux rencontrés, dans l'ordre temporel (avec doublons conservés)
    """
    # Fenêtre GLOBALE : ordre par temps + id d'arrivée (en cas d'ex-aequo)
    wg = Window.orderBy("timestamp", "arrival_id")

    ordered2 = (ordered_global
        .withColumn("prev_type", F.lag("breakdown_type").over(wg))                 # Type précédent global
        .withColumn(
            "is_new_run_global",
            F.when(F.col("prev_type").isNull() | (F.col("prev_type") != F.col("breakdown_type")), 1).otherwise(0)  # Nouveau run si début ou changement de type
        )
    )

    # run_id_global = somme cumulée des indicateurs de nouveau run
    wg_cum = wg.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ordered2 = ordered2.withColumn("run_id_global", F.sum("is_new_run_global").over(wg_cum))

    # Agrégation par run global, en conservant l'ordre des événements via un tri sur (timestamp, arrival_id)
    runs_seq = (
        ordered2.groupBy("run_id_global", "breakdown_type")
        .agg(
            F.count("*").alias("Count_Successive"),
            F.min("timestamp").alias("run_start"),
            # On collecte une séquence ordonnable grâce à un struct(timestamp, arrival_id, level)
            F.array_sort(F.collect_list(F.struct("timestamp","arrival_id","breakdown_level_int"))).alias("seq")
        )
        .withColumn(
            "Breakdowns_Level",
            F.expr("array_join(transform(seq, x -> cast(x.breakdown_level_int as string)), ',')")  # Map vers niveaux puis join CSV
        )
        .select(
            F.col("breakdown_type").alias("Breakdown_Type"),
            "Count_Successive",
            "Breakdowns_Level",
            "run_start"
        )
        .orderBy("run_start")   # Trie final par début de run pour remettre dans l'ordre chronologique
        .drop("run_start")      # On retire la colonne technique non demandée en sortie
    )
    return runs_seq

# ----------------------------------------------------------
# 5) Écritures "tout-en-un"
# ----------------------------------------------------------
def write_outputs(ordered, runs, summary, bad, output_dir, bad_dir):
    """
    Produit tous les artefacts de sortie en mode "tout-en-un":
      - Parquets intermédiaires (parsed, runs, summary)
      - CSV plats : final.csv, exo_output.csv, runs.csv, exo_runs_output.csv
      - Bad lines en .text dans bad_dir
    Nettoie d'abord les répertoires pour éviter les mélanges.
    """
    # Nettoyage des répertoires d'output parquets
    for path in [f"{output_dir}/parsed", f"{output_dir}/runs", f"{output_dir}/summary"]:
        if os.path.exists(path):
            shutil.rmtree(path)

    # Nettoyage soft du répertoire des "bad lines" (on supprime fichiers/sous-dossiers)
    if os.path.exists(bad_dir):
        for filename in os.listdir(bad_dir):
            p = os.path.join(bad_dir, filename)
            try:
                if os.path.isfile(p) or os.path.islink(p):
                    os.unlink(p)
                elif os.path.isdir(p):
                    shutil.rmtree(p)
            except Exception:
                pass  # On ignore les erreurs de suppression pour ne pas bloquer le pipeline

    # Écritures Parquet (résultats intermédiaires utiles pour debug/relectures)
    ordered.write.mode("overwrite").parquet(f"{output_dir}/parsed")
    runs_for_csv = runs.drop("levels_in_run") if "levels_in_run" in runs.columns else runs  # Sécurité si cette colonne existait
    if "levels_in_run_int" in runs_for_csv.columns:
        runs_for_csv = runs_for_csv.drop("levels_in_run_int")  # On ne conserve que la version string
    runs_for_csv.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/runs", header=True)
    tmp = summary
    for c in ["distinct_levels", "distinct_levels_int"]:
        if c in tmp.columns:
            tmp = tmp.drop(c)
    tmp.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/summary", header=True)

    # Bad lines (texte brut) marquées par un suffixe temporel pour l'historique
    bad.coalesce(1).write.mode("overwrite").text(f"{bad_dir}/bad_lines_{int(time.time())}")

    # Fichiers plats "simples" (CSV unique) via utilitaire write_single_csv

    # 1) final.csv : résumé complet par type (sans colonnes techniques distinct_levels[_int])
    summary_final = summary
    for c in ["distinct_levels", "distinct_levels_int"]:
        if c in summary_final.columns:
            summary_final = summary_final.drop(c)
    write_single_csv(summary_final, f"{output_dir}/final.csv")

    # 2) exo_output.csv : format réduit (par type) attendu par l'exercice
    exo = summary_final.select(
        "breakdown_type",
        "max_run_length",
        F.col("distinct_levels_str").alias("levels")
    )
    write_single_csv(exo, f"{output_dir}/exo_output.csv")

    # 3) runs.csv : détail par type (run_id, longueur, bornes, niveaux)
    runs_detail = runs.select(
        "breakdown_type", "run_id", "run_length", "run_start", "run_end", "levels_in_run_str"
    )
    write_single_csv(runs_detail, f"{output_dir}/runs.csv")

    # 4) exo_runs_output.csv : runs globaux dans la timeline (format prof)
    ordered_global = good_order_for_global(ordered)         # Sécurité : on s'assure du tri global (timestamp, arrival_id)
    runs_seq = compute_global_runs_for_exo(ordered_global)  # Calcule les runs successifs globaux
    write_single_csv(runs_seq, f"{output_dir}/exo_runs_output.csv")

# Petite fonction utilitaire : re-tri global pour sécurité/stabilité

def good_order_for_global(ordered_df):
    # sécurité: retrier au cas où
    return ordered_df.orderBy("timestamp", "arrival_id")   # Tri global pour toutes les lignes

# ----------------------------------------------------------
# 6) Exécution "par étapes"
# ----------------------------------------------------------
def stage_read(spark, input_path, stage_dir, bad_dir):
    """
    Étape READ :
      - Lit et parse les lignes brutes, split good/bad
      - Écrit good en Parquet dans stage_dir/good.parquet
      - Écrit les "bad lines" en texte dans bad_dir
    """
    raw = read_raw(spark, input_path)            # Lecture brute
    parsed = parse_fields(raw)                   # Parsing des champs
    good, bad = split_good_bad(parsed)           # Séparation des bonnes et mauvaises lignes
    os.makedirs(stage_dir, exist_ok=True)        # S'assure que le dossier de stage existe
    good.write.mode("overwrite").parquet(f"{stage_dir}/good.parquet")     # Persiste le bon jeu pour l'étape suivante
    bad.write.mode("overwrite").text(f"{bad_dir}/bad_lines_{int(time.time())}")  # Sauvegarde des lignes rejetées

def stage_sort(spark, stage_dir):
    """
    Étape SORT :
      - Charge good.parquet
      - Trie globalement par (timestamp, arrival_id)
      - Écrit stage_dir/sorted.parquet
    """
    good = spark.read.parquet(f"{stage_dir}/good.parquet")                        # Recharge les données valides
    sorted_df = good.orderBy("timestamp", "arrival_id")                         # Tri global stable
    sorted_df.write.mode("overwrite").parquet(f"{stage_dir}/sorted.parquet")    # Persistance pour l'étape compute

def stage_compute(spark, stage_dir):
    """
    Étape COMPUTE :
      - Charge sorted.parquet
      - Calcule ordered/runs/summary (mêmes logiques que le mode tout-en-un)
      - Écrit ordered.parquet, runs.parquet, summary.parquet
    """
    sorted_df = spark.read.parquet(f"{stage_dir}/sorted.parquet")                # Données triées
    ordered, runs, summary = compute_runs_and_summary_partitioned(sorted_df)      # Calculs principaux
    ordered.write.mode("overwrite").parquet(f"{stage_dir}/ordered.parquet")     # Sauvegardes parquet
    runs.write.mode("overwrite").parquet(f"{stage_dir}/runs.parquet")
    summary.write.mode("overwrite").parquet(f"{stage_dir}/summary.parquet")

def stage_save(spark, stage_dir, final_csv_path):
    """
    Étape SAVE :
      - À partir des parquets calculés, produit les CSV finaux (final.csv, exo_output.csv, runs.csv, exo_runs_output.csv)
      - Utilise write_single_csv pour avoir un fichier unique par sortie
    """
    out_dir = os.path.dirname(final_csv_path)                                      # Répertoire d'output commun

    # final.csv (résumé par type)
    summary = spark.read.parquet(f"{stage_dir}/summary.parquet")                 # Recharge le summary parquet
    for c in ["distinct_levels", "distinct_levels_int"]:
        if c in summary.columns:
            summary = summary.drop(c)                                              # Nettoie les colonnes techniques si présentes
    write_single_csv(summary, final_csv_path)                                      # Écrit le CSV final

    # exo_output.csv (format réduit)
    exo = summary.select(
        "breakdown_type",
        "max_run_length",
        F.col("distinct_levels_str").alias("levels")
    )
    write_single_csv(exo, f"{out_dir}/exo_output.csv")

    # runs.csv (détail des runs par type)
    runs = spark.read.parquet(f"{stage_dir}/runs.parquet").select(
        "breakdown_type", "run_id", "run_length", "run_start", "run_end", "levels_in_run_str"
    )
    write_single_csv(runs, f"{out_dir}/runs.csv")

    # exo_runs_output.csv (runs globaux = format prof)
    ordered = spark.read.parquet(f"{stage_dir}/ordered.parquet")                 # Recharge l'ordered parquet
    ordered = good_order_for_global(ordered)                                       # Tri global par sûreté
    runs_seq = compute_global_runs_for_exo(ordered)                                # Calcule les runs globaux successifs
    write_single_csv(runs_seq, f"{out_dir}/exo_runs_output.csv")                 # Écrit le CSV demandé

# ----------------------------------------------------------
# 7) main
# ----------------------------------------------------------
def main():
    """
    Point d'entrée CLI. Paramètres :
      --input     : chemin du fichier brut (obligatoire)
      --output    : dossier de sortie pour le mode tout-en-un (obligatoire en tout-en-un)
      --bad       : dossier pour stocker les "bad lines" (obligatoire)
      --stage     : si fourni, exécute une des étapes [read|sort|compute|save]
      --stage_dir : dossier de travail pour les étapes (par défaut /opt/airflow/stage)
      --final_csv : chemin final.csv quand on est en mode --stage save

    Exemples d'utilisation :
      # Mode tout-en-un
      #   python jobs/breakdown_job.py --input /opt/airflow/data/break_down.txt \
      #          --output /opt/airflow/output --bad /opt/airflow/bad

      # Mode par étapes
      #   python jobs/breakdown_job.py --stage read --input ... --stage_dir ... --bad ...
      #   python jobs/breakdown_job.py --stage sort --stage_dir ...
      #   python jobs/breakdown_job.py --stage compute --stage_dir ...
      #   python jobs/breakdown_job.py --stage save --stage_dir ... --final_csv /opt/airflow/output/final.csv
    """
    parser = argparse.ArgumentParser()                                            # Définition du parseur CLI
    parser.add_argument("--input", required=True, help="fichier brut (break_down.txt)")
    parser.add_argument("--output", required=True, help="dossier de sortie (mode tout-en-un)")
    parser.add_argument("--bad", required=True, help="dossier bad lines")
    parser.add_argument("--stage", choices=["read","sort","compute","save"])         # Étape optionnelle
    parser.add_argument("--stage_dir", default="/opt/airflow/stage")
    parser.add_argument("--final_csv", default="/opt/airflow/output/final.csv")

    args = parser.parse_args()                                                    # Parse les arguments
    spark = spark_session()                                                       # Crée la session Spark

    if args.stage:  # exécution 'étape'
        os.makedirs(args.stage_dir, exist_ok=True)                                # S'assure que le stage_dir existe
        if args.stage == "read":
            stage_read(spark, args.input, args.stage_dir, args.bad)
        elif args.stage == "sort":
            stage_sort(spark, args.stage_dir)
        elif args.stage == "compute":
            stage_compute(spark, args.stage_dir)
        elif args.stage == "save":
            stage_save(spark, args.stage_dir, args.final_csv)
        spark.stop()                                                              # Libère proprement la session Spark
        return

    # mode tout-en-un
    raw = read_raw(spark, args.input)                                             # 1) lecture brute
    parsed = parse_fields(raw)                                                    # 2) parsing
    good, bad = split_good_bad(parsed)                                            # 3) split good/bad
    ordered, runs, summary = compute_runs_and_summary_partitioned(good)           # 4) calculs par type
    write_outputs(ordered, runs, summary, bad, args.output, args.bad)             # 5) écritures de toutes les sorties
    spark.stop()                                                                  # Arrêt de Spark en fin de job

# Point d'entrée standard en exécution directe
if __name__ == "__main__":
    main()  # Appel de la fonction main
