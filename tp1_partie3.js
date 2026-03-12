// 3.1.1 Identifiez les heures de la journée (0-23h) où le nombre de fraudes est le plus élevé. Présentez les résultats triés par nombre de fraudes décroissant.

    db.transactions.aggregate([
        { $match: { Fraud_Label: "Fraud" } },
        {
            $group: {
                _id: { $toInt: { $substr: ["$Transaction_Time", 0, 2] } },
                fraudCount: { $sum: 1 }
                }
            },
        { $sort: { fraudCount: -1 } },
        {
            $project: {
                _id: 0,
                hour: "$_id",
                fraudCount: 1
                }
            }
        ])

    // Résultat : Les 24 heures triées par nombre de fraudes décroissant. On identifie les heures les plus risquées.

    // 3.1.2 Trouvez les clients qui ont effectué plus de 10 transactions en une seule journée ET dont au moins une transaction a été frauduleuse. Affichez leur Customer_ID et le nombre total de transactions.

    db.transactions.aggregate([
        {
            $group: {
                _id: {
                    customer: "$Customer_ID",
                    day: "$Transaction_Date"
                    },
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        {
            $match: {
                totalTransactions: { $gt: 10 },
                fraudCount: { $gte: 1 }
                }
            },
        { $sort: { totalTransactions: -1 } },
        {
            $project: {
                _id: 0,
                Customer_ID: "$_id.customer",
                date: "$_id.day",
                totalTransactions: 1
                }
            }
        ])

    // Résultat : Liste des clients avec plus de 10 transactions en un jour et au moins 1 fraude.

    // 3.2.1 Identifiez les 5 localisations de transactions (Transaction_Location) avec le taux de fraude le plus élevé. Pour chaque localisation, affichez le nom, le nombre total de transactions, le nombre de fraudes et le taux de fraude en pourcentage.

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Transaction_Location",
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        {
            $project: {
                _id: 0,
                location: "$_id",
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: {
                    $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100]
                    }
                }
            },
        { $sort: { fraudRate: -1 } },
        { $limit: 5 }
        ])

    // Résultat : Les 5 localisations avec le taux de fraude le plus élevé, avec nombre de transactions et taux en %.

    // 3.2.2 Trouvez toutes les transactions où la localisation de transaction est différente de la localisation du domicile du client (Customer_Home_Location), avec une distance supérieure à 200 km. Calculez le taux de fraude pour ces transactions "à distance".

    db.transactions.aggregate([
        {
            $match: {
                $expr: { $ne: ["$Transaction_Location", "$Customer_Home_Location"] },
                Distance_From_Home: { $gt: 200 }
                }
            },
        {
            $group: {
                _id: null,
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        {
            $project: {
                _id: 0,
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: {
                    $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100]
                    }
                }
            }
        ])

    // Résultat : On obtient le nombre de transactions "à distance" (localisation ≠ domicile et > 200km) et le taux de fraude parmi celles-ci.

    // 3.3.1 Identifiez les 10 marchands (Merchant_ID) avec le montant total de transactions frauduleuses le plus élevé. Pour chaque marchand, calculez le montant total des fraudes, le nombre de fraudes et le montant moyen par fraude.

    db.transactions.aggregate([
        { $match: { Fraud_Label: "Fraud" } },
        {
            $group: {
                _id: "$Merchant_ID",
                totalFraudAmount: { $sum: "$Transaction_Amount (in Million)" },
                fraudCount: { $sum: 1 },
                avgFraudAmount: { $avg: "$Transaction_Amount (in Million)" }
                }
            },
        { $sort: { totalFraudAmount: -1 } },
        {
            $project: {
                _id: 0,
                Merchant_ID: "$_id",
                totalFraudAmount: 1,
                fraudCount: 1,
                avgFraudAmount: 1
                }
            },
        { $limit: 10 }
        ])

    // Résultat : Les 10 marchands avec le plus gros montant total de fraudes, avec nombre de fraudes et montant moyen par fraude.

    // 3.3.2 Trouvez les catégories de marchands (Merchant_Category) où les clients utilisent préférentiellement des cartes de crédit vs des cartes de débit. Présentez les résultats avec le ratio crédit/débit pour chaque catégorie.

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Merchant_Category",
                creditCount: { $sum: { $cond: [{ $eq: ["$Card_Type", "Credit"] }, 1, 0] } },
                debitCount: { $sum: { $cond: [{ $eq: ["$Card_Type", "Debit"] }, 1, 0] } }
                }
            },
        { $sort: { creditDebitRatio: -1 } },
        {
            $project: {
                _id: 0,
                category: "$_id",
                creditCount: 1,
                debitCount: 1,
                creditDebitRatio: {
                    $cond: [
                        { $eq: ["$debitCount", 0] },
                        "Aucun débit",
                        { $round: [{ $divide: ["$creditCount", "$debitCount"] }, 2] }
                        ]
                    }
                }
            }
        ])

    // Résultat : Le ratio crédit/débit pour chaque catégorie de marchand, trié par ratio décroissant. Un ratio > 1 signifie que le crédit est préféré.

    // 3.4.1 Identifiez les clients dont le montant de transaction actuel dépasse de plus de 300% leur montant moyen de transaction (Avg_Transaction_Amount). Quel est le taux de fraude pour ces "transactions exceptionnelles" ?

    db.transactions.aggregate([
        {
            $match: {
                $expr: {
                    $gt: [
                        "$Transaction_Amount (in Million)",
                        { $multiply: ["$Avg_Transaction_Amount (in Million)", 3] }
                        ]
                    }
                }
            },
        {
            $group: {
                _id: null,
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        {
            $project: {
                _id: 0,
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: {
                    $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100]
                    }
                }
            }
        ])

    // Résultat : Nombre de transactions dépassant 300% de la moyenne du client et taux de fraude parmi celles-ci.

    // 3.4.2 Trouvez les transactions où le client a utilisé un nouveau marchand (Is_New_Merchant) ET où c'est une transaction internationale. Analysez si ces facteurs combinés sont des indicateurs forts de fraude.

    db.transactions.aggregate([
        {
            $match: {
                Is_New_Merchant: true,
                Is_International_Transaction: true
                }
            },
        {
            $group: {
                _id: null,
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        {
            $project: {
                _id: 0,
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: {
                    $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100]
                    }
                }
            }
        ])

    // Résultat : Nombre de transactions avec nouveau marchand + international, et taux de fraude. Permet d'évaluer si ces deux facteurs combinés sont un bon indicateur de fraude.

    // 3.4.3 Créez une requête qui identifie les "transactions suspectes" selon les critères suivants (au moins 3 doivent être vrais) : Montant > 2x la moyenne du client, Transaction à heure inhabituelle, Nouveau marchand, Transaction internationale, Distance du domicile > 100 km, Plus de 5 transactions dans la journée. Combien de transactions correspondent ? Quel est leur taux de fraude ?

    db.transactions.aggregate([
        {
            $addFields: {
                suspicionScore: {
                    $sum: [
                        { $cond: [{ $gt: ["$Transaction_Amount (in Million)", { $multiply: ["$Avg_Transaction_Amount (in Million)", 2] }] }, 1, 0] },
                        { $cond: [{ $eq: ["$Unusual_Time_Transaction", true] }, 1, 0] },
                        { $cond: [{ $eq: ["$Is_New_Merchant", true] }, 1, 0] },
                        { $cond: [{ $eq: ["$Is_International_Transaction", true] }, 1, 0] },
                        { $cond: [{ $gt: ["$Distance_From_Home", 100] }, 1, 0] },
                        { $cond: [{ $gt: ["$Daily_Transaction_Count", 5] }, 1, 0] }
                        ]
                    }
                }
            },
        { $match: { suspicionScore: { $gte: 3 } } },
        {
            $group: {
                _id: null,
                totalSuspicious: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        {
            $project: {
                _id: 0,
                totalSuspicious: 1,
                fraudCount: 1,
                fraudRate: {
                    $multiply: [{ $divide: ["$fraudCount", "$totalSuspicious"] }, 100]
                    }
                }
            }
        ])

    // Résultat : Nombre de transactions suspectes (au moins 3 critères vrais sur 6) et leur taux de fraude. Plus le score est élevé, plus la transaction est suspecte.