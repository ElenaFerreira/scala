# Analyse de Fraude Bancaire avec Apache Spark

> **Travaux Pratiques - M2TL**  
> Auteur : Elena Ferreira  
> Technologies : Scala 2.12 · Apache Spark 3.5.7


## Description

Ce projet implémente une **analyse exploratoire de données transactionnelles bancaires** pour la détection de comportements frauduleux. Il utilise Apache Spark pour traiter un dataset de plus de **13 millions de transactions**.

L'objectif est d'identifier des patterns suspects à travers une approche statistique (sans Machine Learning) en analysant les montants, la temporalité, les catégories de marchands et les erreurs de transaction.


## Structure du Projet

```
TP3/
├── exercice.scala          # Code principal de l'analyse
├── synthese.txt            # Réponses détaillées aux questions du TP
├── README.md               
└── dataset/
    ├── transactions_data.csv   # 13,3M transactions bancaires
    ├── cards_data.csv          # Données des cartes (4 071 cartes)
    ├── users_data.csv          # Profils clients (1 219 utilisateurs)
    ├── mcc_codes.json          # Codes catégories marchands
    └── train_fraud_labels.json # Labels de fraude (pour ML)
```

## Exécution

### Prérequis

- **Scala CLI** (ou scala-cli)
- Java 11+

### Lancer l'analyse

```bash
scala-cli run exercice.scala
```


## Fonctionnalités

### Partie 1 — Prise en Main des Données (EDA)

| Exercice | Fonction | Description |
|----------|----------|-------------|
| 1 | `chargementDonnees()` | Chargement CSV/JSON avec inférence de schéma |
| 2 | `analyseVolumetrie()` | Comptages : transactions, clients, cartes, marchands |
| 3 | `analyseQualiteDonnees()` | Détection des valeurs nulles, montants ≤ 0, erreurs |

### Partie 2 — Analyse des Montants & Comportements

| Exercice | Fonction | Description |
|----------|----------|-------------|
| 4 | `analyseMontants()` | Statistiques descriptives, quartiles, distribution par tranche |
| 5 | `analyseTemporelle()` | Répartition par heure, jour, mois |

### Partie 3 — Enrichissement Métier

| Exercice | Fonction | Description |
|----------|----------|-------------|
| 6 | `jointureMCC()` | Jointure avec codes MCC, top catégories |
| 7 | `analyseErreurs()` | Types d'erreurs, taux par carte et client |

### Partie 4 — Détection de Fraude

| Exercice | Fonction | Description |
|----------|----------|-------------|
| 8 | `creationIndicateurs()` | Indicateurs par carte : nb transactions, montants, villes |
| 9 | `detectionSuspects()` | Détection basée sur seuils (multi-critères) |


## Résultats Clés

### Volumétrie

| Métrique | Valeur |
|----------|--------|
| Transactions | **13 305 915** |
| Clients uniques | 1 219 |
| Cartes uniques | 4 071 |
| Commerçants | 74 831 |

### Distribution des Montants

| Statistique | Valeur |
|-------------|--------|
| Moyenne | 42,98 € |
| Médiane | 28,77 € |
| Min / Max | -500 € / 6 820 € |
| % < 50 € | 66,5% |
| % > 200 € | 2,44% |

### Patterns Temporels

- **Creux nocturne** (1h-5h) : seulement 5% du volume
- **Pic journée** (12h) : maximum d'activité
- Distribution journalière homogène (~1,9M/jour)

### Catégories à Risque

| Catégorie | Risque | Raison |
|-----------|--------|--------|
| Money Transfer | 🔴 Très élevé | Blanchiment, irréversible |
| Industries métallurgiques | 🟠 Élevé | Montants ~780€, profil atypique |
| Cruise Lines / Airlines | 🟡 Modéré | Montants élevés |

### Erreurs

- Taux global : **1,59%**
- Top erreur : `Insufficient Balance` (62%)
- Erreurs suspectes : `Bad PIN`, `Bad CVV`, `Bad Card Number`


## Critères de Détection

Les seuils suivants sont utilisés pour identifier les cartes suspectes :

```scala
SEUIL_TX_PAR_JOUR = 10          // Plus de 10 transactions/jour
SEUIL_NB_VILLES = 3             // Plus de 3 villes différentes
SEUIL_MONTANT_JOURNALIER = 1000 // Montant total > 1000€/jour
```

## Technologies

- **Apache Spark SQL** : traitement distribué des DataFrames
- **Scala 2.12** : langage fonctionnel typé
- **Scala CLI** : gestionnaire de dépendances et build

## Références

- [Documentation Apache Spark](https://spark.apache.org/docs/latest/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)