// 5.1.1 Créez un pipeline d'agrégation qui calcule, pour chaque type de carte (Card_Type), le montant total des transactions, le montant moyen, et le nombre de transactions. Triez par montant total décroissant.

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Card_Type",
                totalAmount: { $sum: "$Transaction_Amount (in Million)" },
                avgAmount: { $avg: "$Transaction_Amount (in Million)" },
                totalTransactions: { $sum: 1 }
                }
            },
        { $sort: { totalAmount: -1 } },
        {
            $project: {
                _id: 0,
                cardType: "$_id",
                totalAmount: 1,
                avgAmount: { $round: ["$avgAmount", 2] },
                totalTransactions: 1
                }
            }
        ])

    // Résultat : On obtient les stats par type de carte (Visa, MasterCard, etc.) triées par montant total décroissant.

    // 5.1.2 Calculez pour chaque catégorie de marchand : le nombre total de transactions, le nombre de fraudes, le taux de fraude (en %), le montant moyen des fraudes. Affichez uniquement les catégories avec un taux de fraude supérieur à 10%.

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Merchant_Category",
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
                totalFraudAmount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount (in Million)", 0] } }
                }
            },
        {
            $addFields: {
                fraudRate: { $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100] },
                avgFraudAmount: {
                    $cond: [
                        { $eq: ["$fraudCount", 0] },
                        0,
                        { $divide: ["$totalFraudAmount", "$fraudCount"] }
                        ]
                    }
                }
            },
        { $match: { fraudRate: { $gt: 10 } } },
        { $sort: { fraudRate: -1 } },
        {
            $project: {
                _id: 0,
                category: "$_id",
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: { $round: ["$fraudRate", 2] },
                avgFraudAmount: { $round: ["$avgFraudAmount", 2] }
                }
            }
        ])

    // Résultat : Seules les catégories avec un taux de fraude > 10% sont affichées, triées par taux décroissant.

    // 5.1.3 Identifiez les 20 clients ayant le solde de compte le plus élevé (Account_Balance). Pour chacun, affichez leur Customer_ID, leur solde, et le nombre total de leurs transactions.

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Customer_ID",
                accountBalance: { $max: "$Account_Balance (in Million)" },
                totalTransactions: { $sum: 1 }
                }
            },
        { $sort: { accountBalance: -1 } },
        {
            $project: {
                _id: 0,
                Customer_ID: "$_id",
                accountBalance: 1,
                totalTransactions: 1
                }
            },
        { $limit: 20 }
        ])

    // Résultat : Les 20 clients avec le solde le plus élevé, avec leur nombre de transactions.

    // 5.2.1 Créez une analyse hebdomadaire des fraudes : pour chaque semaine, calculez le nombre total de transactions, le nombre de fraudes, le montant total des fraudes, le montant moyen par transaction frauduleuse.

    db.transactions.aggregate([
        {
            $group: {
                _id: { $isoWeek: "$Transaction_Date" },
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
                totalFraudAmount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount (in Million)", 0] } }
                }
            },
        {
            $addFields: {
                avgFraudAmount: {
                    $cond: [
                        { $eq: ["$fraudCount", 0] },
                        0,
                        { $round: [{ $divide: ["$totalFraudAmount", "$fraudCount"] }, 2] }
                        ]
                    }
                }
            },
        { $sort: { _id: 1 } },
        {
            $project: {
                _id: 0,
                week: "$_id",
                totalTransactions: 1,
                fraudCount: 1,
                totalFraudAmount: 1,
                avgFraudAmount: 1
                }
            }
        ])

    // Résultat : Pour chaque semaine (1 à 52), on obtient le total de transactions, le nombre de fraudes, le montant total et moyen des fraudes.

    // 5.2.2 Analysez le comportement des clients selon leur historique de fraude : Groupe 1 (Previous_Fraud_Count = 0), Groupe 2 (1-2), Groupe 3 (> 2). Pour chaque groupe, calculez le taux de fraude actuel et le montant moyen des transactions.

    db.transactions.aggregate([
        {
            $addFields: {
                riskGroup: {
                    $switch: {
                        branches: [
                            { case: { $eq: ["$Previous_Fraud_Count", 0] }, then: "Groupe 1 - Clients propres" },
                            { case: { $lte: ["$Previous_Fraud_Count", 2] }, then: "Groupe 2 - Risque modéré" }
                            ],
                        default: "Groupe 3 - Haut risque"
                        }
                    }
                }
            },
        {
            $group: {
                _id: "$riskGroup",
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
                avgAmount: { $avg: "$Transaction_Amount (in Million)" }
                }
            },
        { $sort: { _id: 1 } },
        {
            $project: {
                _id: 0,
                riskGroup: "$_id",
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: {
                    $round: [{ $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100] }, 2]
                    },
                avgAmount: { $round: ["$avgAmount", 2] }
                }
            }
        ])

    // Résultat : Les 3 groupes sont affichés avec leur taux de fraude et montant moyen. Le Groupe 3 (haut risque) a logiquement le taux de fraude le plus élevé.

    // 5.2.3 Créez un pipeline qui identifie les "heures de pointe de fraude". Pour chaque heure de la journée (0-23h), calculez le ratio (fraudes/transactions totales). Identifiez les 5 heures les plus risquées.

    db.transactions.aggregate([
        {
            $group: {
                _id: { $toInt: { $substr: ["$Transaction_Time", 0, 2] } },
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } }
                }
            },
        { $sort: { fraudRate: -1 } },
        {
            $project: {
                _id: 0,
                hour: "$_id",
                totalTransactions: 1,
                fraudCount: 1,
                fraudRate: {
                    $round: [{ $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100] }, 2]
                    }
                }
            },
        { $limit: 5 }
        ])

    // Résultat : Les 5 heures les plus risquées pour la fraude, avec leur ratio fraudes/transactions.

    // 5.3.1 Créez une collection merchants contenant des informations fictives sur au moins 10 marchands. Puis créez un pipeline qui joint les transactions avec les informations des marchands et affiche un rapport complet pour les transactions frauduleuses.

    db.merchants.insertMany([
        { Merchant_ID: 97028, name: "TechWorld Electronics", address: "12 Orchard Rd, Singapore", category: "Electronics", opening_date: new Date("2018-03-15") },
        { Merchant_ID: 27515, name: "QuickCash ATM Services", address: "45 Mall Rd, Lahore", category: "ATM", opening_date: new Date("2015-07-20") },
        { Merchant_ID: 13810, name: "Digital Hub", address: "78 Susan Rd, Faisalabad", category: "Electronics", opening_date: new Date("2020-01-10") },
        { Merchant_ID: 10501, name: "FreshMart Grocery", address: "23 Baker St, London", category: "Grocery", opening_date: new Date("2019-06-01") },
        { Merchant_ID: 50234, name: "FuelStop Express", address: "90 Highway Blvd, Karachi", category: "Fuel", opening_date: new Date("2017-11-25") },
        { Merchant_ID: 61002, name: "Fashion Palace", address: "34 Oxford St, London", category: "Clothing", opening_date: new Date("2016-04-18") },
        { Merchant_ID: 72310, name: "Golden Jewels", address: "56 Grand Bazaar, Istanbul", category: "Jewelry", opening_date: new Date("2014-09-30") },
        { Merchant_ID: 83045, name: "Gourmet Kitchen", address: "12 Champs-Elysées, Paris", category: "Restaurant", opening_date: new Date("2021-02-14") },
        { Merchant_ID: 94100, name: "PharmaCare Plus", address: "67 Health Ave, Dubai", category: "Healthcare", opening_date: new Date("2018-08-05") },
        { Merchant_ID: 15678, name: "AutoParts Direct", address: "89 Industrial Zone, Riyadh", category: "Automotive", opening_date: new Date("2019-12-01") }
        ])

    db.transactions.aggregate([
        { $match: { Fraud_Label: "Fraud" } },
        {
            $lookup: {
                from: "merchants",
                localField: "Merchant_ID",
                foreignField: "Merchant_ID",
                as: "merchantInfo"
                }
            },
        { $unwind: { path: "$merchantInfo", preserveNullAndEmptyArrays: true } },
        {
            $project: {
                _id: 0,
                Transaction_ID: 1,
                Customer_ID: 1,
                "Transaction_Amount (in Million)": 1,
                Transaction_Date: 1,
                Merchant_ID: 1,
                merchantName: "$merchantInfo.name",
                merchantAddress: "$merchantInfo.address",
                Fraud_Label: 1
                }
            },
        { $limit: 20 }
        ])

    // Résultat : Les 20 premières transactions frauduleuses avec les infos du marchand (nom, adresse) jointes via $lookup. Les transactions sans marchand correspondant affichent null grâce à preserveNullAndEmptyArrays.

    // 5.3.2 Créez une collection customers avec des informations fictives sur les clients. Créez ensuite un pipeline qui génère un "profil de risque client" comprenant : informations client, nombre total de transactions, nombre de fraudes historiques, montant moyen de transaction, score de risque calculé.

    db.customers.insertMany([
        { Customer_ID: 24239, name: "Ali Khan", email: "ali.khan@email.com", city: "Lahore", registration_date: new Date("2020-01-15") },
        { Customer_ID: 77250, name: "Sara Ahmed", email: "sara.a@email.com", city: "Lahore", registration_date: new Date("2019-06-20") },
        { Customer_ID: 34294, name: "Hassan Malik", email: "h.malik@email.com", city: "Faisalabad", registration_date: new Date("2021-03-10") },
        { Customer_ID: 92041, name: "Fatima Noor", email: "f.noor@email.com", city: "Karachi", registration_date: new Date("2018-11-05") },
        { Customer_ID: 55123, name: "Omar Raza", email: "o.raza@email.com", city: "Islamabad", registration_date: new Date("2020-08-22") },
        { Customer_ID: 68450, name: "Zara Iqbal", email: "z.iqbal@email.com", city: "Rawalpindi", registration_date: new Date("2017-04-30") },
        { Customer_ID: 41087, name: "Bilal Shah", email: "b.shah@email.com", city: "Peshawar", registration_date: new Date("2022-01-01") },
        { Customer_ID: 89234, name: "Ayesha Tariq", email: "a.tariq@email.com", city: "Multan", registration_date: new Date("2019-09-15") },
        { Customer_ID: 13567, name: "Usman Ghani", email: "u.ghani@email.com", city: "Quetta", registration_date: new Date("2021-07-08") },
        { Customer_ID: 76890, name: "Nadia Hussain", email: "n.hussain@email.com", city: "Hyderabad", registration_date: new Date("2020-12-20") }
        ])

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Customer_ID",
                totalTransactions: { $sum: 1 },
                fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
                avgTransactionAmount: { $avg: "$Transaction_Amount (in Million)" },
                maxPreviousFraud: { $max: "$Previous_Fraud_Count" }
                }
            },
        {
            $lookup: {
                from: "customers",
                localField: "_id",
                foreignField: "Customer_ID",
                as: "customerInfo"
                }
            },
        { $unwind: { path: "$customerInfo", preserveNullAndEmptyArrays: true } },
        {
            $addFields: {
                riskScore: {
                    $sum: [
                        { $cond: [{ $gt: ["$fraudCount", 5] }, 30, { $cond: [{ $gt: ["$fraudCount", 2] }, 20, { $cond: [{ $gt: ["$fraudCount", 0] }, 10, 0] }] }] },
                        { $cond: [{ $gt: ["$maxPreviousFraud", 2] }, 30, { $cond: [{ $gt: ["$maxPreviousFraud", 0] }, 15, 0] }] },
                        { $cond: [{ $gt: ["$avgTransactionAmount", 7] }, 20, { $cond: [{ $gt: ["$avgTransactionAmount", 5] }, 10, 0] }] },
                        { $cond: [{ $gt: ["$totalTransactions", 50] }, 20, { $cond: [{ $gt: ["$totalTransactions", 20] }, 10, 0] }] }
                        ]
                    }
                }
            },
        { $sort: { riskScore: -1 } },
        {
            $project: {
                _id: 0,
                Customer_ID: "$_id",
                customerName: "$customerInfo.name",
                customerCity: "$customerInfo.city",
                totalTransactions: 1,
                fraudCount: 1,
                avgTransactionAmount: { $round: ["$avgTransactionAmount", 2] },
                riskScore: 1
                }
            },
        { $limit: 20 }
        ])

    // Résultat : Profil de risque des 20 clients les plus risqués avec leur score calculé, nombre de fraudes et montant moyen. Les clients présents dans la collection customers ont aussi leur nom et ville.

    // 5.4.1 Créez un pipeline qui identifie les transactions ayant un "score de suspicion" élevé : Transaction internationale (+3), Nouveau marchand (+2), Heure inhabituelle (+2), Distance > 100km (+2), Montant > 2x moyenne client (+3), Failed_Transaction_Count > 0 (+1 par échec). Affichez les 50 transactions avec le score le plus élevé et vérifiez combien sont réellement frauduleuses.

    db.transactions.aggregate([
        {
            $addFields: {
                suspicionScore: {
                    $sum: [
                        { $cond: [{ $eq: ["$Is_International_Transaction", true] }, 3, 0] },
                        { $cond: [{ $eq: ["$Is_New_Merchant", true] }, 2, 0] },
                        { $cond: [{ $eq: ["$Unusual_Time_Transaction", true] }, 2, 0] },
                        { $cond: [{ $gt: ["$Distance_From_Home", 100] }, 2, 0] },
                        { $cond: [{ $gt: ["$Transaction_Amount (in Million)", { $multiply: ["$Avg_Transaction_Amount (in Million)", 2] }] }, 3, 0] },
                        { $cond: [{ $gt: ["$Failed_Transaction_Count", 0] }, "$Failed_Transaction_Count", 0] }
                        ]
                    }
                }
            },
        { $sort: { suspicionScore: -1 } },
        {
            $project: {
                _id: 0,
                Transaction_ID: 1,
                Customer_ID: 1,
                "Transaction_Amount (in Million)": 1,
                Fraud_Label: 1,
                suspicionScore: 1
                }
            },
        { $limit: 50 }
        ])

    // Résultat : Les 50 transactions avec le score de suspicion le plus élevé. On peut vérifier combien ont Fraud_Label = "Fraud" pour évaluer la pertinence du scoring.

    // 5.4.2 Créez une vue matérialisée (collection calculée avec $out) nommée daily_fraud_stats qui agrège les statistiques quotidiennes : Date, Nombre de transactions, Nombre de fraudes, Montant total des fraudes, Taux de fraude, Top 3 des catégories de marchands par nombre de fraudes.

    db.transactions.aggregate([
        {
            $facet: {
                dailyStats: [
                    {
                        $group: {
                            _id: "$Transaction_Date",
                            totalTransactions: { $sum: 1 },
                            fraudCount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] } },
                            totalFraudAmount: { $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, "$Transaction_Amount (in Million)", 0] } }
                            }
                        },
                    {
                        $addFields: {
                            fraudRate: {
                                $round: [{ $multiply: [{ $divide: ["$fraudCount", "$totalTransactions"] }, 100] }, 2]
                                }
                            }
                        },
                    { $sort: { _id: 1 } }
                    ],
                topCategories: [
                    { $match: { Fraud_Label: "Fraud" } },
                    {
                        $group: {
                            _id: {
                                date: "$Transaction_Date",
                                category: "$Merchant_Category"
                                },
                            fraudCount: { $sum: 1 }
                            }
                        },
                    { $sort: { "_id.date": 1, fraudCount: -1 } },
                    {
                        $group: {
                            _id: "$_id.date",
                            topCategories: { $push: { category: "$_id.category", fraudCount: "$fraudCount" } }
                            }
                        },
                    {
                        $addFields: {
                            topCategories: { $slice: ["$topCategories", 3] }
                            }
                        }
                    ]
                }
            },
        { $unwind: "$dailyStats" },
        {
            $addFields: {
                matchingTop: {
                    $filter: {
                        input: "$topCategories",
                        as: "t",
                        cond: { $eq: ["$$t._id", "$dailyStats._id"] }
                        }
                    }
                }
            },
        {
            $project: {
                _id: 0,
                date: "$dailyStats._id",
                totalTransactions: "$dailyStats.totalTransactions",
                fraudCount: "$dailyStats.fraudCount",
                totalFraudAmount: "$dailyStats.totalFraudAmount",
                fraudRate: "$dailyStats.fraudRate",
                topFraudCategories: { $arrayElemAt: ["$matchingTop.topCategories", 0] }
                }
            },
        { $sort: { date: 1 } },
        { $out: "daily_fraud_stats" }
        ])

    db.daily_fraud_stats.find().limit(5)

    // Résultat : La collection daily_fraud_stats est créée via $out. Elle contient pour chaque jour : le nombre de transactions, le nombre de fraudes, le montant total, le taux de fraude et le top 3 des catégories de marchands frauduleuses.