//> using scala "2.12.18"
//> using dep "org.apache.spark:spark-sql_2.12:3.5.7"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.nio=ALL-UNNAMED"

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}

object ExerciceTP {

  // ============================================================================
  // UTILITAIRES
  // ============================================================================

  def requireFile(path: String): Unit = {
    if (!Files.exists(Paths.get(path))) {
      throw new IllegalArgumentException(s"Fichier introuvable: $path")
    }
  }

  def inspect(df: DataFrame, name: String): Unit = {
    println(s"\n===== $name =====")
    df.printSchema()
    df.show(10, truncate = false)
    println(s"Nombre de colonnes : ${df.columns.length}")
  }

  // ============================================================================
  // PARTIE 1 – PRISE EN MAIN DES DONNÉES (EDA BRUTE)
  // ============================================================================

  // --------------------------------------------------------------------------
  // Exercice 1 : Chargement des données
  // --------------------------------------------------------------------------
  def chargementDonnees(spark: SparkSession, basePath: String): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    println("\n" + "=" * 80)
    println("PARTIE 1 – PRISE EN MAIN DES DONNÉES")
    println("=" * 80)
    println("\n>>> Exercice 1 : Chargement des données")

    val transactionsPath = s"$basePath/transactions_data.csv"
    val cardsPath        = s"$basePath/cards_data.csv"
    val usersPath        = s"$basePath/users_data.csv"
    val mccPath          = s"$basePath/mcc_codes.json"

    requireFile(transactionsPath)
    requireFile(cardsPath)
    requireFile(usersPath)
    requireFile(mccPath)

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "PERMISSIVE")
      .csv(transactionsPath)

    val cards = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "PERMISSIVE")
      .csv(cardsPath)

    val users = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "PERMISSIVE")
      .csv(usersPath)

    val mccRaw = spark.read
      .option("multiLine", "true")
      .json(mccPath)

    // Transformation du JSON MCC (pivot pour avoir mcc -> category)
    val n = mccRaw.columns.length
    val expr =
      s"stack($n, " +
        mccRaw.columns.map(c => s"'$c', `$c`").mkString(", ") +
      ") as (mcc, merchant_category)"
    val mcc = mccRaw.selectExpr(expr)

    inspect(transactions, "transactions_data")
    inspect(cards, "cards_data")
    inspect(users, "users_data")
    inspect(mcc, "mcc_codes (transformé)")

    (transactions, cards, users, mcc)
  }

  // --------------------------------------------------------------------------
  // Exercice 2 : Analyse de volumétrie
  // --------------------------------------------------------------------------
  def analyseVolumetrie(transactions: DataFrame): Unit = {
    println("\n>>> Exercice 2 : Analyse de volumétrie")

    val totalTransactions = transactions.count()
    val uniqueClients = transactions.select(col("client_id")).distinct().count()
    val uniqueCards = transactions.select(col("card_id")).distinct().count()
    val uniqueMerchants = transactions.select(col("merchant_id")).distinct().count()

    println(s"Nombre total de transactions : $totalTransactions")
    println(s"Nombre de clients uniques     : $uniqueClients")
    println(s"Nombre de cartes uniques      : $uniqueCards")
    println(s"Nombre de commerçants uniques : $uniqueMerchants")
  }

  // --------------------------------------------------------------------------
  // Exercice 3 : Qualité des données
  // --------------------------------------------------------------------------
  def analyseQualiteDonnees(spark: SparkSession, transactions: DataFrame): Unit = {
    import spark.implicits._

    println("\n>>> Exercice 3 : Qualité des données")

    val totalTransactions = transactions.count()

    // 3.1 Tableau récapitulatif des valeurs nulles par colonne
    println("\n--- Tableau récapitulatif des valeurs nulles ---")
    
    val schema = transactions.schema
    
    val nullCounts = transactions.columns.map { colName =>
      val colType = schema(colName).dataType.typeName
      val isNumericType = colType == "double" || colType == "float"
      
      val nullCount = if (isNumericType) {
        transactions.filter(col(colName).isNull || col(colName).isNaN).count()
      } else if (colType == "string") {
        transactions.filter(col(colName).isNull || col(colName) === "").count()
      } else {
        transactions.filter(col(colName).isNull).count()
      }
      
      val percentage = if (totalTransactions > 0) (nullCount.toDouble / totalTransactions * 100) else 0.0
      (colName, colType, nullCount, f"$percentage%.2f%%")
    }

    val nullStatsDF = nullCounts.toSeq.toDF("colonne", "type", "nb_valeurs_manquantes", "pourcentage")
    nullStatsDF.show(truncate = false)

    // 3.2 Transactions avec montant ≤ 0
    println("\n--- Transactions avec montant ≤ 0 ---")
    
    val transactionsWithAmount = transactions.withColumn(
      "amount_clean",
      regexp_replace(col("amount"), "\\$", "").cast("double")
    )

    val transactionsMontantNegatif = transactionsWithAmount
      .filter(col("amount_clean") <= 0)
    
    val countMontantNegatif = transactionsMontantNegatif.count()
    val pourcentageMontantNegatif = (countMontantNegatif.toDouble / totalTransactions * 100)
    
    println(s"Nombre de transactions avec montant ≤ 0 : $countMontantNegatif")
    println(f"Pourcentage : $pourcentageMontantNegatif%.2f%%")
    println("Exemples :")
    transactionsMontantNegatif.select("id", "amount", "amount_clean", "client_id", "card_id").show(10, truncate = false)

    // 3.3 Transactions sans MCC
    println("\n--- Transactions sans MCC ---")
    
    val transactionsSansMCC = transactions.filter(col("mcc").isNull)
    
    val countSansMCC = transactionsSansMCC.count()
    val pourcentageSansMCC = (countSansMCC.toDouble / totalTransactions * 100)
    
    println(s"Nombre de transactions sans MCC : $countSansMCC")
    println(f"Pourcentage : $pourcentageSansMCC%.2f%%")
    if (countSansMCC > 0) {
      println("Exemples :")
      transactionsSansMCC.show(10, truncate = false)
    }

    // 3.4 Transactions contenant des erreurs
    println("\n--- Transactions contenant des erreurs ---")
    
    val transactionsAvecErreurs = transactions
      .filter(col("errors").isNotNull && col("errors") =!= "")
    
    val countAvecErreurs = transactionsAvecErreurs.count()
    val pourcentageAvecErreurs = (countAvecErreurs.toDouble / totalTransactions * 100)
    
    println(s"Nombre de transactions avec erreurs : $countAvecErreurs")
    println(f"Pourcentage : $pourcentageAvecErreurs%.2f%%")
    
    println("\nRépartition des types d'erreurs :")
    transactionsAvecErreurs
      .groupBy("errors")
      .agg(count("*").alias("nb_occurrences"))
      .orderBy(desc("nb_occurrences"))
      .show(20, truncate = false)

    // 3.5 Résumé global
    println("\n--- RÉSUMÉ QUALITÉ DES DONNÉES ---")
    println(s"Total de transactions           : $totalTransactions")
    println(s"Transactions montant ≤ 0        : $countMontantNegatif (${f"$pourcentageMontantNegatif%.2f"}%)")
    println(s"Transactions sans MCC           : $countSansMCC (${f"$pourcentageSansMCC%.2f"}%)")
    println(s"Transactions avec erreurs       : $countAvecErreurs (${f"$pourcentageAvecErreurs%.2f"}%)")
  }

  // ============================================================================
  // PARTIE 2 – ANALYSE DES MONTANTS & COMPORTEMENTS
  // ============================================================================

  // --------------------------------------------------------------------------
  // Exercice 4 : Analyse des montants
  // --------------------------------------------------------------------------
  def analyseMontants(spark: SparkSession, transactions: DataFrame): DataFrame = {
    import spark.implicits._

    println("\n" + "=" * 80)
    println("PARTIE 2 – ANALYSE DES MONTANTS & COMPORTEMENTS")
    println("=" * 80)
    println("\n>>> Exercice 4 : Analyse des montants")

    // Nettoyage du montant (suppression du $ et conversion en Double)
    val transactionsClean = transactions.withColumn(
      "amount_clean",
      regexp_replace(col("amount"), "\\$", "").cast("double")
    )

    // 4.1 Statistiques descriptives : moyenne, médiane, min, max
    println("\n--- Statistiques des montants ---")
    
    val stats = transactionsClean.agg(
      avg("amount_clean").alias("moyenne"),
      min("amount_clean").alias("minimum"),
      max("amount_clean").alias("maximum"),
      count("amount_clean").alias("total")
    )
    stats.show(truncate = false)

    // Calcul de la médiane (percentile 50%)
    val mediane = transactionsClean.stat.approxQuantile("amount_clean", Array(0.5), 0.01)(0)
    println(f"Médiane : $mediane%.2f €")

    // Quartiles pour mieux comprendre la distribution
    val quartiles = transactionsClean.stat.approxQuantile("amount_clean", Array(0.25, 0.5, 0.75), 0.01)
    println(f"\nQuartiles :")
    println(f"  Q1 (25%%) : ${quartiles(0)}%.2f €")
    println(f"  Q2 (50%%) : ${quartiles(1)}%.2f €")
    println(f"  Q3 (75%%) : ${quartiles(2)}%.2f €")

    // 4.2 Distribution par tranche
    println("\n--- Distribution par tranche de montant ---")

    val transactionsWithTranche = transactionsClean.withColumn(
      "tranche",
      when(col("amount_clean") < 10, "< 10 €")
        .when(col("amount_clean") >= 10 && col("amount_clean") < 50, "10 - 50 €")
        .when(col("amount_clean") >= 50 && col("amount_clean") < 200, "50 - 200 €")
        .otherwise(">= 200 €")
    )

    val totalTransactions = transactionsClean.count()

    val distributionTranches = transactionsWithTranche
      .groupBy("tranche")
      .agg(
        count("*").alias("nb_transactions"),
        round(avg("amount_clean"), 2).alias("montant_moyen"),
        round(sum("amount_clean"), 2).alias("montant_total")
      )
      .withColumn("pourcentage", round(col("nb_transactions") / totalTransactions * 100, 2))
      .orderBy(
        when(col("tranche") === "< 10 €", 1)
          .when(col("tranche") === "10 - 50 €", 2)
          .when(col("tranche") === "50 - 200 €", 3)
          .otherwise(4)
      )

    distributionTranches.show(truncate = false)

    // Retourne le DataFrame nettoyé pour les analyses suivantes
    transactionsClean
  }

  // --------------------------------------------------------------------------
  // Exercice 5 : Analyse temporelle
  // --------------------------------------------------------------------------
  def analyseTemporelle(spark: SparkSession, transactions: DataFrame): Unit = {
    import spark.implicits._

    println("\n>>> Exercice 5 : Analyse temporelle")

    // 5.1 Extraction de l'heure, du jour de la semaine et du mois
    val transactionsTemporel = transactions
      .withColumn("heure", hour(col("date")))
      .withColumn("jour_semaine", dayofweek(col("date")))
      .withColumn("jour_nom", 
        when(dayofweek(col("date")) === 1, "Dimanche")
          .when(dayofweek(col("date")) === 2, "Lundi")
          .when(dayofweek(col("date")) === 3, "Mardi")
          .when(dayofweek(col("date")) === 4, "Mercredi")
          .when(dayofweek(col("date")) === 5, "Jeudi")
          .when(dayofweek(col("date")) === 6, "Vendredi")
          .otherwise("Samedi")
      )
      .withColumn("mois", month(col("date")))

    // Aperçu des colonnes temporelles extraites
    println("\n--- Aperçu des colonnes temporelles ---")
    transactionsTemporel.select("date", "heure", "jour_nom", "mois").show(10, truncate = false)

    // 5.2 Nombre de transactions par heure
    println("\n--- Nombre de transactions par heure ---")
    val transactionsParHeure = transactionsTemporel
      .groupBy("heure")
      .agg(count("*").alias("nb_transactions"))
      .orderBy("heure")

    transactionsParHeure.show(24, truncate = false)

    // 5.3 Nombre de transactions par jour de la semaine
    println("\n--- Nombre de transactions par jour de la semaine ---")
    val transactionsParJour = transactionsTemporel
      .groupBy("jour_semaine", "jour_nom")
      .agg(count("*").alias("nb_transactions"))
      .orderBy("jour_semaine")
      .select("jour_nom", "nb_transactions")

    transactionsParJour.show(7, truncate = false)

    // 5.4 Nombre de transactions par mois
    println("\n--- Nombre de transactions par mois ---")
    val transactionsParMois = transactionsTemporel
      .groupBy("mois")
      .agg(count("*").alias("nb_transactions"))
      .orderBy("mois")

    transactionsParMois.show(12, truncate = false)
  }

  // ============================================================================
  // PARTIE 3 – ENRICHISSEMENT MÉTIER (MCC & ERREURS)
  // ============================================================================

  // --------------------------------------------------------------------------
  // Exercice 6 : Jointure avec les MCC
  // --------------------------------------------------------------------------
  def jointureMCC(spark: SparkSession, transactions: DataFrame, mcc: DataFrame): DataFrame = {
    import spark.implicits._

    println("\n" + "=" * 80)
    println("PARTIE 3 – ENRICHISSEMENT MÉTIER")
    println("=" * 80)
    println("\n>>> Exercice 6 : Jointure avec les MCC")

    // 6.1 Jointure transactions + mcc_codes pour ajouter merchant_category
    // Le mcc dans transactions est un Integer, dans mcc c'est un String, donc on cast
    val transactionsEnrichies = transactions
      .withColumn("mcc_str", col("mcc").cast("string"))
      .join(mcc, col("mcc_str") === mcc("mcc"), "left")
      .drop("mcc_str")
      .drop(mcc("mcc"))

    println("\n--- Aperçu des transactions enrichies ---")
    transactionsEnrichies.select("id", "mcc", "merchant_category", "amount").show(10, truncate = false)

    // Nettoyage du montant pour les calculs
    val transactionsAvecMontant = transactionsEnrichies.withColumn(
      "amount_clean",
      regexp_replace(col("amount"), "\\$", "").cast("double")
    )

    // 6.2 Top 10 des catégories par volume (nombre de transactions)
    println("\n--- Top 10 des catégories par volume ---")
    val top10Volume = transactionsAvecMontant
      .filter(col("merchant_category").isNotNull)
      .groupBy("merchant_category")
      .agg(count("*").alias("nb_transactions"))
      .orderBy(desc("nb_transactions"))
      .limit(10)

    top10Volume.show(truncate = false)

    // 6.3 Montant moyen par catégorie
    println("\n--- Montant moyen par catégorie (top 15) ---")
    val montantMoyenParCategorie = transactionsAvecMontant
      .filter(col("merchant_category").isNotNull)
      .groupBy("merchant_category")
      .agg(
        round(avg("amount_clean"), 2).alias("montant_moyen"),
        count("*").alias("nb_transactions"),
        round(sum("amount_clean"), 2).alias("montant_total")
      )
      .orderBy(desc("montant_moyen"))
      .limit(15)

    montantMoyenParCategorie.show(truncate = false)

    // Retourne le DataFrame enrichi pour les analyses suivantes
    transactionsEnrichies
  }

  // --------------------------------------------------------------------------
  // Exercice 7 : Analyse des erreurs
  // --------------------------------------------------------------------------
  def analyseErreurs(spark: SparkSession, transactions: DataFrame): Unit = {
    import spark.implicits._

    println("\n>>> Exercice 7 : Analyse des erreurs")

    val totalTransactions = transactions.count()

    // 7.1 Types d'erreurs les plus fréquents
    println("\n--- Types d'erreurs les plus fréquents ---")
    
    val transactionsAvecErreurs = transactions
      .filter(col("errors").isNotNull && col("errors") =!= "")

    val countAvecErreurs = transactionsAvecErreurs.count()
    println(s"Transactions avec erreurs : $countAvecErreurs / $totalTransactions")

    val erreursFrequentes = transactionsAvecErreurs
      .groupBy("errors")
      .agg(count("*").alias("nb_occurrences"))
      .withColumn("pourcentage", round(col("nb_occurrences") / countAvecErreurs * 100, 2))
      .orderBy(desc("nb_occurrences"))

    erreursFrequentes.show(20, truncate = false)

    // 7.2 Taux d'erreur par carte
    println("\n--- Taux d'erreur par carte (top 20 cartes avec le plus d'erreurs) ---")
    
    val statsParCarte = transactions
      .groupBy("card_id")
      .agg(
        count("*").alias("total_transactions"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
      )
      .withColumn("taux_erreur", round(col("nb_erreurs") / col("total_transactions") * 100, 2))
      .orderBy(desc("nb_erreurs"))

    statsParCarte.show(20, truncate = false)

    // Cartes avec taux d'erreur 100%
    val cartesProblematiques = statsParCarte.filter(col("taux_erreur") === 100)
    println(s"Nombre de cartes avec 100% d'erreurs : ${cartesProblematiques.count()}")

    // 7.3 Taux d'erreur par client
    println("\n--- Taux d'erreur par client (top 20 clients avec le plus d'erreurs) ---")
    
    val statsParClient = transactions
      .groupBy("client_id")
      .agg(
        count("*").alias("total_transactions"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).alias("nb_erreurs")
      )
      .withColumn("taux_erreur", round(col("nb_erreurs") / col("total_transactions") * 100, 2))
      .orderBy(desc("nb_erreurs"))

    statsParClient.show(20, truncate = false)

    // Distribution des taux d'erreur par client
    println("\n--- Distribution des taux d'erreur par client ---")
    val distributionTauxErreur = statsParClient
      .withColumn("tranche_taux",
        when(col("taux_erreur") === 0, "0%")
          .when(col("taux_erreur") < 5, "0-5%")
          .when(col("taux_erreur") < 10, "5-10%")
          .when(col("taux_erreur") < 25, "10-25%")
          .otherwise(">= 25%")
      )
      .groupBy("tranche_taux")
      .agg(count("*").alias("nb_clients"))
      .orderBy(
        when(col("tranche_taux") === "0%", 1)
          .when(col("tranche_taux") === "0-5%", 2)
          .when(col("tranche_taux") === "5-10%", 3)
          .when(col("tranche_taux") === "10-25%", 4)
          .otherwise(5)
      )

    distributionTauxErreur.show(truncate = false)
  }

  // ============================================================================
  // PARTIE 4 – APPROCHE FRAUDE (SANS MACHINE LEARNING)
  // ============================================================================

  // --------------------------------------------------------------------------
  // Exercice 8 : Création d'indicateurs
  // --------------------------------------------------------------------------
  def creationIndicateurs(spark: SparkSession, transactions: DataFrame): DataFrame = {
    import spark.implicits._

    println("\n" + "=" * 80)
    println("PARTIE 4 – APPROCHE FRAUDE (SANS MACHINE LEARNING)")
    println("=" * 80)
    println("\n>>> Exercice 8 : Création d'indicateurs")

    // Nettoyage du montant et extraction de la date (jour)
    val transactionsPrep = transactions
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
      .withColumn("jour", to_date(col("date")))
      .withColumn("has_error", when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0))

    // 8.1 Nombre de transactions par carte et par jour
    println("\n--- Indicateur 1 : Nombre de transactions par carte et par jour ---")
    
    val txParCarteJour = transactionsPrep
      .groupBy("card_id", "jour")
      .agg(count("*").alias("nb_tx_jour"))
      .orderBy(desc("nb_tx_jour"))

    txParCarteJour.show(15, truncate = false)

    // 8.2 Montant total par carte et par jour
    println("\n--- Indicateur 2 : Montant total par carte et par jour ---")
    
    val montantParCarteJour = transactionsPrep
      .groupBy("card_id", "jour")
      .agg(round(sum("amount_clean"), 2).alias("montant_total_jour"))
      .orderBy(desc("montant_total_jour"))

    montantParCarteJour.show(15, truncate = false)

    // 8.3 Nombre de villes différentes utilisées par carte
    println("\n--- Indicateur 3 : Nombre de villes différentes par carte ---")
    
    val villesParCarte = transactionsPrep
      .filter(col("merchant_city").isNotNull && col("merchant_city") =!= "" && col("merchant_city") =!= "ONLINE")
      .groupBy("card_id")
      .agg(countDistinct("merchant_city").alias("nb_villes_distinctes"))
      .orderBy(desc("nb_villes_distinctes"))

    villesParCarte.show(15, truncate = false)

    // 8.4 Ratio de transactions avec erreur (par carte)
    println("\n--- Indicateur 4 : Ratio de transactions avec erreur par carte ---")
    
    val ratioErreursParCarte = transactionsPrep
      .groupBy("card_id")
      .agg(
        count("*").alias("total_tx"),
        sum("has_error").alias("nb_erreurs"),
        round(sum("has_error") / count("*") * 100, 2).alias("ratio_erreur_pct")
      )
      .orderBy(desc("ratio_erreur_pct"))

    ratioErreursParCarte.show(15, truncate = false)

    // Création d'un DataFrame consolidé avec tous les indicateurs par carte
    println("\n--- Tableau consolidé des indicateurs par carte ---")
    
    val indicateursParCarte = transactionsPrep
      .groupBy("card_id")
      .agg(
        count("*").alias("total_transactions"),
        round(sum("amount_clean"), 2).alias("montant_total"),
        round(avg("amount_clean"), 2).alias("montant_moyen"),
        countDistinct("jour").alias("nb_jours_actifs"),
        countDistinct("merchant_city").alias("nb_villes"),
        sum("has_error").alias("nb_erreurs"),
        round(sum("has_error") / count("*") * 100, 2).alias("ratio_erreur_pct")
      )
      .orderBy(desc("total_transactions"))

    indicateursParCarte.show(20, truncate = false)

    indicateursParCarte
  }

  // --------------------------------------------------------------------------
  // Exercice 9 : Détection de comportements suspects
  // --------------------------------------------------------------------------
  def detectionSuspects(spark: SparkSession, transactions: DataFrame): DataFrame = {
    import spark.implicits._

    println("\n>>> Exercice 9 : Détection de comportements suspects")

    // Seuils de détection (ajustables)
    val SEUIL_TX_PAR_JOUR = 10        // Plus de X transactions par jour
    val SEUIL_NB_VILLES = 3           // Plus de 3 villes différentes
    val SEUIL_MONTANT_JOURNALIER = 1000.0  // Montant total journalier élevé

    println(s"\nSeuils utilisés :")
    println(s"  - Transactions par jour > $SEUIL_TX_PAR_JOUR")
    println(s"  - Nombre de villes > $SEUIL_NB_VILLES")
    println(s"  - Montant journalier > $SEUIL_MONTANT_JOURNALIER €")

    // Préparation des données
    val transactionsPrep = transactions
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
      .withColumn("jour", to_date(col("date")))

    // Critère 1 : Cartes avec plus de X transactions par jour
    println("\n--- Critère 1 : Cartes avec trop de transactions par jour ---")
    
    val cartesMultiTx = transactionsPrep
      .groupBy("card_id", "jour")
      .agg(count("*").alias("nb_tx_jour"))
      .filter(col("nb_tx_jour") > SEUIL_TX_PAR_JOUR)
      .select("card_id")
      .distinct()

    println(s"Cartes avec > $SEUIL_TX_PAR_JOUR tx/jour : ${cartesMultiTx.count()}")

    // Critère 2 : Cartes avec transactions dans plus de 3 villes
    println("\n--- Critère 2 : Cartes utilisées dans trop de villes ---")
    
    val cartesMultiVilles = transactionsPrep
      .filter(col("merchant_city").isNotNull && col("merchant_city") =!= "" && col("merchant_city") =!= "ONLINE")
      .groupBy("card_id")
      .agg(countDistinct("merchant_city").alias("nb_villes"))
      .filter(col("nb_villes") > SEUIL_NB_VILLES)
      .select("card_id")
      .distinct()

    println(s"Cartes avec > $SEUIL_NB_VILLES villes : ${cartesMultiVilles.count()}")

    // Critère 3 : Cartes avec montant total journalier élevé
    println("\n--- Critère 3 : Cartes avec montant journalier élevé ---")
    
    val cartesMontantEleve = transactionsPrep
      .groupBy("card_id", "jour")
      .agg(sum("amount_clean").alias("montant_jour"))
      .filter(col("montant_jour") > SEUIL_MONTANT_JOURNALIER)
      .select("card_id")
      .distinct()

    println(s"Cartes avec montant jour > $SEUIL_MONTANT_JOURNALIER € : ${cartesMontantEleve.count()}")

    // Union de toutes les cartes suspectes
    val suspicious_cards = cartesMultiTx
      .union(cartesMultiVilles)
      .union(cartesMontantEleve)
      .distinct()

    println(s"\n--- TOTAL CARTES SUSPECTES : ${suspicious_cards.count()} ---")

    // Détail des cartes suspectes avec leurs indicateurs
    println("\n--- Détail des cartes suspectes ---")
    
    val detailSuspects = transactionsPrep
      .groupBy("card_id")
      .agg(
        count("*").alias("total_tx"),
        round(sum("amount_clean"), 2).alias("montant_total"),
        countDistinct("jour").alias("nb_jours"),
        countDistinct("merchant_city").alias("nb_villes"),
        round(max(col("amount_clean")), 2).alias("montant_max")
      )
      .join(suspicious_cards, Seq("card_id"), "inner")
      .withColumn("tx_par_jour_moy", round(col("total_tx") / col("nb_jours"), 2))
      .withColumn("raison_suspect",
        concat_ws(", ",
          when(col("tx_par_jour_moy") > SEUIL_TX_PAR_JOUR, lit("Trop de tx/jour")),
          when(col("nb_villes") > SEUIL_NB_VILLES, lit("Multi-villes")),
          when(col("montant_total") / col("nb_jours") > SEUIL_MONTANT_JOURNALIER, lit("Montant élevé"))
        )
      )
      .orderBy(desc("total_tx"))

    detailSuspects.show(30, truncate = false)

    suspicious_cards
  }

  // ============================================================================
  // BONUS – FONCTIONNALITÉS AVANCÉES
  // ============================================================================

  // --------------------------------------------------------------------------
  // Bonus 1 : Score de risque simple
  // --------------------------------------------------------------------------
  def calculScoreRisque(spark: SparkSession, transactions: DataFrame): DataFrame = {
    import spark.implicits._

    println("\n" + "=" * 80)
    println("BONUS – FONCTIONNALITÉS AVANCÉES")
    println("=" * 80)
    println("\n>>> Bonus 1 : Calcul du score de risque")

    // Préparation des données
    val transactionsPrep = transactions
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
      .withColumn("jour", to_date(col("date")))
      .withColumn("heure", hour(col("date")))
      .withColumn("has_error", when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0))

    // Calcul des indicateurs par client
    val indicateursClient = transactionsPrep
      .groupBy("client_id")
      .agg(
        count("*").alias("nb_transactions"),
        round(sum("amount_clean"), 2).alias("montant_total"),
        round(avg("amount_clean"), 2).alias("montant_moyen"),
        round(max("amount_clean"), 2).alias("montant_max"),
        countDistinct("card_id").alias("nb_cartes"),
        countDistinct("merchant_city").alias("nb_villes"),
        countDistinct("jour").alias("nb_jours_actifs"),
        sum("has_error").alias("nb_erreurs"),
        // Transactions nocturnes (entre 0h et 6h)
        sum(when(col("heure") >= 0 && col("heure") < 6, 1).otherwise(0)).alias("nb_tx_nocturnes"),
        // Transactions en ligne
        sum(when(col("merchant_city") === "ONLINE", 1).otherwise(0)).alias("nb_tx_online"),
        // Nombre de marchands différents
        countDistinct("merchant_id").alias("nb_marchands")
      )

    // Calcul des statistiques globales pour normaliser
    val stats = indicateursClient.agg(
      avg("nb_transactions").alias("avg_tx"),
      stddev("nb_transactions").alias("std_tx"),
      avg("montant_moyen").alias("avg_montant"),
      stddev("montant_moyen").alias("std_montant"),
      avg("nb_villes").alias("avg_villes"),
      avg("nb_erreurs").alias("avg_erreurs")
    ).collect()(0)

    val avgTx = stats.getAs[Double]("avg_tx")
    val avgMontant = stats.getAs[Double]("avg_montant")
    val avgVilles = stats.getAs[Double]("avg_villes")
    val avgErreurs = stats.getAs[Double]("avg_erreurs")

    // Score de risque basé sur plusieurs critères (0-100)
    // Chaque critère contribue au score final
    val clientsAvecScore = indicateursClient
      .withColumn("score_volume",
        // Score basé sur le volume de transactions (max 20 points)
        least(lit(20), col("nb_transactions") / avgTx * 10)
      )
      .withColumn("score_montant",
        // Score basé sur le montant moyen élevé (max 20 points)
        least(lit(20), col("montant_moyen") / avgMontant * 10)
      )
      .withColumn("score_geographique",
        // Score basé sur la dispersion géographique (max 20 points)
        least(lit(20), col("nb_villes") / avgVilles * 10)
      )
      .withColumn("score_erreurs",
        // Score basé sur le taux d'erreur (max 20 points)
        least(lit(20), (col("nb_erreurs") / col("nb_transactions")) * 100)
      )
      .withColumn("score_nocturne",
        // Score basé sur les transactions nocturnes (max 10 points)
        least(lit(10), (col("nb_tx_nocturnes") / col("nb_transactions")) * 50)
      )
      .withColumn("score_diversite",
        // Score basé sur la diversité des marchands (max 10 points)
        least(lit(10), col("nb_marchands") / col("nb_transactions") * 20)
      )
      .withColumn("score_risque",
        round(
          col("score_volume") +
          col("score_montant") +
          col("score_geographique") +
          col("score_erreurs") +
          col("score_nocturne") +
          col("score_diversite"),
          2
        )
      )
      .withColumn("niveau_risque",
        when(col("score_risque") >= 60, "ÉLEVÉ")
          .when(col("score_risque") >= 40, "MOYEN")
          .when(col("score_risque") >= 20, "FAIBLE")
          .otherwise("TRÈS FAIBLE")
      )

    // Affichage des résultats
    println("\n--- Distribution des scores de risque ---")
    clientsAvecScore
      .groupBy("niveau_risque")
      .agg(
        count("*").alias("nb_clients"),
        round(avg("score_risque"), 2).alias("score_moyen"),
        round(avg("montant_total"), 2).alias("montant_total_moyen")
      )
      .orderBy(desc("score_moyen"))
      .show(truncate = false)

    println("\n--- Top 20 clients à risque élevé ---")
    clientsAvecScore
      .filter(col("niveau_risque") === "ÉLEVÉ")
      .select(
        "client_id", "score_risque", "niveau_risque",
        "nb_transactions", "montant_total", "nb_cartes",
        "nb_villes", "nb_erreurs", "nb_tx_nocturnes"
      )
      .orderBy(desc("score_risque"))
      .show(20, truncate = false)

    println("\n--- Détail des composantes du score (Top 10) ---")
    clientsAvecScore
      .select(
        col("client_id"), col("score_risque"),
        round(col("score_volume"), 1).alias("vol"),
        round(col("score_montant"), 1).alias("mnt"),
        round(col("score_geographique"), 1).alias("geo"),
        round(col("score_erreurs"), 1).alias("err"),
        round(col("score_nocturne"), 1).alias("noc"),
        round(col("score_diversite"), 1).alias("div")
      )
      .orderBy(desc("score_risque"))
      .show(10, false)

    clientsAvecScore
  }

  // --------------------------------------------------------------------------
  // Bonus 2 : Sauvegarde en format Parquet
  // --------------------------------------------------------------------------
  def sauvegarderParquet(
    spark: SparkSession,
    clientsAvecScore: DataFrame,
    suspiciousCards: DataFrame,
    transactions: DataFrame,
    outputPath: String = "output"
  ): Unit = {
    import spark.implicits._

    println("\n>>> Bonus 2 : Sauvegarde en format Parquet")

    // Créer le dossier de sortie s'il n'existe pas
    val outputDir = Paths.get(outputPath)
    if (!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
    }

    // 1. Sauvegarder les scores de risque par client
    println(s"\n--- Sauvegarde des scores de risque ---")
    clientsAvecScore
      .select(
        "client_id", "score_risque", "niveau_risque",
        "nb_transactions", "montant_total", "montant_moyen",
        "nb_cartes", "nb_villes", "nb_erreurs"
      )
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/scores_risque")
    println(s"  ✓ Scores sauvegardés dans $outputPath/scores_risque")

    // 2. Sauvegarder les cartes suspectes
    println(s"\n--- Sauvegarde des cartes suspectes ---")
    
    val transactionsPrep = transactions
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
      .withColumn("jour", to_date(col("date")))

    val detailSuspects = transactionsPrep
      .groupBy("card_id")
      .agg(
        first("client_id").alias("client_id"),
        count("*").alias("total_tx"),
        round(sum("amount_clean"), 2).alias("montant_total"),
        countDistinct("merchant_city").alias("nb_villes")
      )
      .join(suspiciousCards, Seq("card_id"), "inner")

    detailSuspects
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/cartes_suspectes")
    println(s"  ✓ Cartes suspectes sauvegardées dans $outputPath/cartes_suspectes")

    // 3. Sauvegarder un résumé par niveau de risque
    println(s"\n--- Sauvegarde du résumé par niveau de risque ---")
    val resumeRisque = clientsAvecScore
      .groupBy("niveau_risque")
      .agg(
        count("*").alias("nb_clients"),
        round(sum("montant_total"), 2).alias("montant_total"),
        round(avg("score_risque"), 2).alias("score_moyen"),
        sum("nb_erreurs").alias("total_erreurs")
      )

    resumeRisque
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(s"$outputPath/resume_risque")
    println(s"  ✓ Résumé sauvegardé dans $outputPath/resume_risque")

    println(s"\n✓ Tous les fichiers Parquet ont été sauvegardés dans '$outputPath/'")
  }

  // --------------------------------------------------------------------------
  // Bonus 3 : Comparaison clients normaux vs suspects
  // --------------------------------------------------------------------------
  def comparaisonClientsNormauxVsSuspects(
    spark: SparkSession,
    transactions: DataFrame,
    clientsAvecScore: DataFrame
  ): Unit = {
    import spark.implicits._

    println("\n>>> Bonus 3 : Comparaison clients normaux vs suspects")

    // Séparation en deux groupes basés sur le niveau de risque
    val clientsSuspects = clientsAvecScore
      .filter(col("niveau_risque").isin("ÉLEVÉ", "MOYEN"))
      .select("client_id")
      .withColumn("est_suspect", lit(true))

    val clientsNormaux = clientsAvecScore
      .filter(col("niveau_risque").isin("FAIBLE", "TRÈS FAIBLE"))
      .select("client_id")
      .withColumn("est_suspect", lit(false))

    val clientsClasses = clientsSuspects.union(clientsNormaux)

    // Préparation des transactions
    val transactionsPrep = transactions
      .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
      .withColumn("heure", hour(col("date")))
      .withColumn("jour_semaine", dayofweek(col("date")))
      .withColumn("has_error", when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0))
      .withColumn("is_online", when(col("merchant_city") === "ONLINE", 1).otherwise(0))
      .withColumn("is_nocturne", when(col("heure") >= 0 && col("heure") < 6, 1).otherwise(0))
      .withColumn("is_weekend", when(col("jour_semaine").isin(1, 7), 1).otherwise(0))

    // Jointure pour classifier les transactions
    val transactionsClassees = transactionsPrep
      .join(clientsClasses, Seq("client_id"), "left")
      .na.fill(false, Seq("est_suspect"))

    // -------------------------------------------------------------------------
    // Comparaison statistique
    // -------------------------------------------------------------------------
    println("\n" + "-" * 60)
    println("COMPARAISON STATISTIQUE : NORMAUX vs SUSPECTS")
    println("-" * 60)

    // 1. Statistiques générales
    println("\n--- 1. Statistiques générales ---")
    val statsGenerales = transactionsClassees
      .groupBy("est_suspect")
      .agg(
        count("*").alias("nb_transactions"),
        countDistinct("client_id").alias("nb_clients"),
        round(avg("amount_clean"), 2).alias("montant_moyen"),
        round(stddev("amount_clean"), 2).alias("ecart_type_montant"),
        round(min("amount_clean"), 2).alias("montant_min"),
        round(max("amount_clean"), 2).alias("montant_max")
      )
      .withColumn("groupe", when(col("est_suspect"), "SUSPECTS").otherwise("NORMAUX"))
      .select("groupe", "nb_clients", "nb_transactions", "montant_moyen", "ecart_type_montant", "montant_min", "montant_max")

    statsGenerales.show(truncate = false)

    // 2. Comportement d'achat
    println("\n--- 2. Comportement d'achat ---")
    val comportementAchat = transactionsClassees
      .groupBy("est_suspect")
      .agg(
        round(avg("amount_clean"), 2).alias("montant_moyen"),
        round(sum("amount_clean") / countDistinct("client_id"), 2).alias("montant_total_par_client"),
        round(count("*").cast("double") / countDistinct("client_id"), 2).alias("tx_par_client"),
        round(avg("is_online") * 100, 2).alias("pct_achats_online"),
        countDistinct("mcc").alias("nb_mcc_distincts")
      )
      .withColumn("groupe", when(col("est_suspect"), "SUSPECTS").otherwise("NORMAUX"))
      .select("groupe", "montant_moyen", "montant_total_par_client", "tx_par_client", "pct_achats_online", "nb_mcc_distincts")

    comportementAchat.show(truncate = false)

    // 3. Comportement temporel
    println("\n--- 3. Comportement temporel ---")
    val comportementTemporel = transactionsClassees
      .groupBy("est_suspect")
      .agg(
        round(avg("is_nocturne") * 100, 2).alias("pct_tx_nocturnes"),
        round(avg("is_weekend") * 100, 2).alias("pct_tx_weekend"),
        round(avg("heure"), 1).alias("heure_moyenne")
      )
      .withColumn("groupe", when(col("est_suspect"), "SUSPECTS").otherwise("NORMAUX"))
      .select("groupe", "pct_tx_nocturnes", "pct_tx_weekend", "heure_moyenne")

    comportementTemporel.show(truncate = false)

    // 4. Taux d'erreur
    println("\n--- 4. Taux d'erreur ---")
    val tauxErreur = transactionsClassees
      .groupBy("est_suspect")
      .agg(
        round(avg("has_error") * 100, 2).alias("taux_erreur_pct"),
        sum("has_error").alias("total_erreurs"),
        count("*").alias("total_transactions")
      )
      .withColumn("groupe", when(col("est_suspect"), "SUSPECTS").otherwise("NORMAUX"))
      .select("groupe", "taux_erreur_pct", "total_erreurs", "total_transactions")

    tauxErreur.show(truncate = false)

    // 5. Distribution géographique
    println("\n--- 5. Distribution géographique ---")
    val distributionGeo = transactionsClassees
      .groupBy("est_suspect")
      .agg(
        countDistinct("merchant_city").alias("nb_villes_distinctes"),
        countDistinct("merchant_state").alias("nb_etats_distincts"),
        round(countDistinct("merchant_city").cast("double") / countDistinct("client_id"), 2).alias("villes_par_client")
      )
      .withColumn("groupe", when(col("est_suspect"), "SUSPECTS").otherwise("NORMAUX"))
      .select("groupe", "nb_villes_distinctes", "nb_etats_distincts", "villes_par_client")

    distributionGeo.show(truncate = false)

    // 6. Distribution des montants par tranche
    println("\n--- 6. Distribution des montants par tranche ---")
    val distributionMontants = transactionsClassees
      .withColumn("tranche_montant",
        when(col("amount_clean") < 10, "< 10 €")
          .when(col("amount_clean") < 50, "10-50 €")
          .when(col("amount_clean") < 200, "50-200 €")
          .when(col("amount_clean") < 500, "200-500 €")
          .otherwise(">= 500 €")
      )
      .groupBy("est_suspect", "tranche_montant")
      .agg(count("*").alias("nb_tx"))
      .withColumn("groupe", when(col("est_suspect"), "SUSPECTS").otherwise("NORMAUX"))

    // Pivot pour avoir une vue côte à côte
    val pivotMontants = distributionMontants
      .groupBy("tranche_montant")
      .pivot("groupe", Seq("NORMAUX", "SUSPECTS"))
      .agg(first("nb_tx"))
      .na.fill(0)
      .orderBy(
        when(col("tranche_montant") === "< 10 €", 1)
          .when(col("tranche_montant") === "10-50 €", 2)
          .when(col("tranche_montant") === "50-200 €", 3)
          .when(col("tranche_montant") === "200-500 €", 4)
          .otherwise(5)
      )

    pivotMontants.show(truncate = false)

    // 7. Top codes MCC par groupe
    println("\n--- 7. Top 10 codes MCC par groupe ---")
    
    println("\nPour les clients NORMAUX :")
    transactionsClassees
      .filter(!col("est_suspect"))
      .filter(col("mcc").isNotNull)
      .groupBy("mcc")
      .agg(count("*").alias("nb_tx"))
      .orderBy(desc("nb_tx"))
      .show(10, truncate = false)

    println("\nPour les clients SUSPECTS :")
    transactionsClassees
      .filter(col("est_suspect"))
      .filter(col("mcc").isNotNull)
      .groupBy("mcc")
      .agg(count("*").alias("nb_tx"))
      .orderBy(desc("nb_tx"))
      .show(10, truncate = false)

    // Résumé final
    println("\n" + "=" * 60)
    println("RÉSUMÉ DES DIFFÉRENCES CLÉS")
    println("=" * 60)
    
    val nbSuspects = clientsSuspects.count()
    val nbNormaux = clientsNormaux.count()
    val totalClients = nbSuspects + nbNormaux
    
    println(s"\n📊 Répartition des clients :")
    println(s"   - Clients normaux   : $nbNormaux (${f"${nbNormaux.toDouble/totalClients*100}%.1f"}%%)")
    println(s"   - Clients suspects  : $nbSuspects (${f"${nbSuspects.toDouble/totalClients*100}%.1f"}%%)")
  }

  // ============================================================================
  // MAIN
  // ============================================================================

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TP Fraud Detection")
      .master("local[*]")
      .config("spark.sql.debug.maxToStringFields", "100")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val basePath = "dataset"

    // -------------------------------------------------------------------------
    // PARTIE 1
    // -------------------------------------------------------------------------
    val (transactions, cards, users, mcc) = chargementDonnees(spark, basePath)
    // analyseVolumetrie(transactions)
    // analyseQualiteDonnees(spark, transactions)

    // -------------------------------------------------------------------------
    // PARTIE 2
    // -------------------------------------------------------------------------
    // val transactionsClean = analyseMontants(spark, transactions)
    // analyseTemporelle(spark, transactionsClean)

    // -------------------------------------------------------------------------
    // PARTIE 3
    // -------------------------------------------------------------------------
    // val transactionsEnrichies = jointureMCC(spark, transactions, mcc)
    // analyseErreurs(spark, transactions)

    // -------------------------------------------------------------------------
    // PARTIE 4
    // -------------------------------------------------------------------------
    // val indicateurs = creationIndicateurs(spark, transactions)
    val suspiciousCards = detectionSuspects(spark, transactions)

    // -------------------------------------------------------------------------
    // BONUS
    // -------------------------------------------------------------------------
    val clientsAvecScore = calculScoreRisque(spark, transactions)
    sauvegarderParquet(spark, clientsAvecScore, suspiciousCards, transactions)
    comparaisonClientsNormauxVsSuspects(spark, transactions, clientsAvecScore)

    spark.stop()
  }
}
