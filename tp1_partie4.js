// 4.1.1 Exécutez la requête suivante et analysez ses performances avec .explain("executionStats") :

    db.transactions.find({
        "Transaction_Amount (in Million)": { $gt: 5 },
        Fraud_Label: "Fraud",
        Is_International_Transaction: true
        }).explain("executionStats")

    // Résultat obtenu "nReturned": new NumberInt("470"), "executionTimeMillis": new NumberInt("46"), "totalDocsExamined": new NumberInt("49227"), "stage": "COLLSCAN"

    // 4.1.2 Sans créer d'index, proposez 3 requêtes fréquentes dans un système de détection de fraude en temps réel. Analysez leurs performances actuelles.

    // Requête 1 : Rechercher les transactions frauduleuses d'un client spécifique
    db.transactions.find({
        Customer_ID: 24239,
        Fraud_Label: "Fraud"
        }).explain("executionStats")

    // Requête 2 : Rechercher les fraudes de montant élevé triées par date
    db.transactions.find({
        Fraud_Label: "Fraud",
        "Transaction_Amount (in Million)": { $gt: 10 }
        }).sort({ Transaction_Date: -1 }).explain("executionStats")

    // Requête 3 : Rechercher les transactions internationales suspectes (heure inhabituelle + distance élevée)
    db.transactions.find({
        Is_International_Transaction: true,
        Unusual_Time_Transaction: true,
        Distance_From_Home: { $gt: 200 }
        }).explain("executionStats")

    // Résultat : Les 3 requêtes effectuent un COLLSCAN et examinent les 49227 documents. Sans index, les performances sont mauvaises pour un système temps réel.

    // 4.2.1 Créez un index sur le champ Fraud_Label. Ré-exécutez la requête de la question 4.1.1 et comparez les performances. Quelle amélioration observez-vous ?

    db.transactions.createIndex({ Fraud_Label: 1 })

    db.transactions.find({
        "Transaction_Amount (in Million)": { $gt: 5 },
        Fraud_Label: "Fraud",
        Is_International_Transaction: true
        }).explain("executionStats")

    // Résultat : On passe de COLLSCAN à IXSCAN. Le nombre de documents examinés diminue (seuls les "Fraud" sont scannés) et le temps d'exécution est réduit.

    // 4.2.2 En vous basant sur la règle ESR (Equality, Sort, Range), créez un index composé optimal pour cette requête. Justifiez l'ordre des champs dans votre index.
    // ESR : Customer_ID (Equality) → Transaction_Date (Sort) → Transaction_Amount (Range)

    db.transactions.createIndex({
        Customer_ID: 1,
        Transaction_Date: -1,
        "Transaction_Amount (in Million)": 1
        })

    db.transactions.find({
        Customer_ID: 24239,
        "Transaction_Amount (in Million)": { $gte: 1, $lte: 10 }
        }).sort({ Transaction_Date: -1 }).explain("executionStats")

    // Résultat : L'index ESR est très efficace. Equality filtre d'abord par Customer_ID, Sort évite un tri en mémoire sur Transaction_Date, Range filtre ensuite sur Amount.

    // 4.2.3 Créez un index qui optimise les recherches par localisation ET catégorie de marchand. Testez son efficacité sur une requête pertinente.

    db.transactions.createIndex({
        Transaction_Location: 1,
        Merchant_Category: 1
        })

    db.transactions.find({
        Transaction_Location: "London",
        Merchant_Category: "Electronics"
        }).explain("executionStats")

    // Résultat : IXSCAN avec totalDocsExamined == nReturned, seuls les documents correspondants sont examinés.

    // 4.2.4 Le champ IP_Address doit être unique pour des raisons de sécurité. Créez l'index approprié. Que se passe-t-il s'il existe des doublons ? Comment gérer cette situation ?

    db.transactions.aggregate([
        {
            $group: {
                _id: "$IP_Address",
                count: { $sum: 1 }
                }
            },
        { $match: { count: { $gt: 1 } } },
        { $sort: { count: -1 } },
        {
            $project: {
                _id: 0,
                IP_Address: "$_id",
                count: 1
                }
            },
        { $limit: 10 }
        ])

    // Résultat : S'il y a des doublons, MongoDB retourne "E11000 duplicate key error". Il faut d'abord supprimer ou modifier les doublons, puis créer l'index unique.

    db.transactions.createIndex(
        { IP_Address: 1 },
        { unique: true }
        )

    // 4.3.1 Créez un index partiel qui indexe uniquement les transactions frauduleuses avec un montant supérieur à 1 million.

    db.transactions.createIndex(
        { "Transaction_Amount (in Million)": 1 },
        {
            partialFilterExpression: {
                Fraud_Label: "Fraud",
                "Transaction_Amount (in Million)": { $gt: 1 }
                }
            }
        )

    db.transactions.find({
        Fraud_Label: "Fraud",
        "Transaction_Amount (in Million)": { $gt: 1 }
        }).explain("executionStats")

    // Résultat : L'index partiel n'indexe que les documents frauduleux avec montant > 1M, il est donc plus petit et plus rapide qu'un index complet.

    // 4.3.2 Certains clients n'ont pas de Previous_Fraud_Count (champ manquant). Créez un index sparse approprié pour ce champ. Quelle est la différence avec un index normal ?

    db.transactions.createIndex(
        { Previous_Fraud_Count: 1 },
        { sparse: true }
        )

    // Résultat : Un index sparse n'indexe que les documents où le champ existe. Un index normal indexerait aussi les documents sans ce champ (valeur null). L'index sparse est donc plus petit mais ne peut pas servir pour les requêtes sur { Previous_Fraud_Count: null }.

    // 4.3.3 Listez tous les index de votre collection avec leurs tailles. Identifiez s'il existe des index inutilisés ou redondants. Proposez un nettoyage.

    db.transactions.getIndexes()

    db.transactions.aggregate([{ $indexStats: {} }])

    db.transactions.stats().indexSizes

    // Résultat : L'index Fraud_Label_1 est potentiellement redondant avec l'index partiel qui filtre déjà sur Fraud_Label. On peut le supprimer avec db.transactions.dropIndex("Fraud_Label_1").

    // 4.4.1 Créez un index qui permet d'exécuter cette requête en tant que "covered query" (requête couverte). Vérifiez avec .explain() que totalDocsExamined est à 0.

    db.transactions.createIndex({
        Customer_ID: 1,
        "Transaction_Amount (in Million)": 1,
        Transaction_Date: 1
        })

    db.transactions.find(
        { Customer_ID: 24239 },
        { Customer_ID: 1, "Transaction_Amount (in Million)": 1, Transaction_Date: 1, _id: 0 }
        ).explain("executionStats")

    // Résultat : Covered query confirmée, totalDocsExamined = 0. MongoDB n'accède pas aux documents car toutes les données demandées sont dans l'index. La projection exclut _id pour que ça fonctionne.
