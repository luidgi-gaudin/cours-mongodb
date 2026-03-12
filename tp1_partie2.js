db.transactions.find()

    //  2.1.1 Trouvez le nombre total de transactions frauduleuses dans le dataset. Comparez-le au nombre total de transactions. Calculez le taux de fraude en pourcentage.

    db.transactions.aggregate([
        {
            $group: {
                _id: null,
                total: {$sum: 1},
                fraudCount: {$sum: {$cond: [{$eq: ["$Fraud_Label", "Fraud"]}, 1, 0]}}
                }
            },
        {
            $project: {
                _id: 0,
                total: 1,
                fraudCount: 1,
                fraudRate:{
                    $multiply:[{$divide: ["$fraudCount", "$total"]}, 100]
                    }
                }
            }
        ])

    //  2.1.2 Identifiez la transaction avec le montant le plus élevé. Est-elle frauduleuse ? Affichez tous ses détails

    db.transactions.aggregate([
        {
            $setWindowFields: {
                output: { maxAmount: { $max: "$Transaction_Amount (in Million)" } }
                }
            },
        { $match: { $expr: { $eq: ["$Transaction_Amount (in Million)", "$maxAmount"] } } },
        { $unset: "maxAmount" },
        { $addFields: { isFraud: { $eq: ["$Fraud_Label", "Fraud"] } } }
        ])

    //  2.1.3 Listez les 10 clients (Customer_ID) ayant effectué le plus grand nombre de transactions. Affichez uniquement leur ID et le nombre de leurs transactions.

    db.transactions.aggregate([
        {
            $group: {
                _id: "$Customer_ID",
                total: {$sum: 1}
                }
            },
        { $sort: { total: -1 } },
        { $project: { _id: 1, total: 1 } },
        { $limit: 10 },
        ])

    //  2.2.1 Trouvez toutes les transactions qui remplissent SIMULTANÉMENT ces critères :
    //Montant supérieur à 5 millions
    //Transaction internationale
    //Effectuée avec une carte de crédit
    //Compte ayant un historique de fraude (Previous_Fraud_Count > 0)
    //Combien de transactions correspondent ? Quel est le taux de fraude parmi celles-ci ?

    db.transactions.aggregate([
        {
            $match: {
                "Transaction_Amount (in Million)": { $gt: 5 },
                "Is_International_Transaction": true,
                "Card_Type": "Credit",
                "Previous_Fraud_Count": { $gt: 0 }
                }
            },
        {
            $group: {
                _id: null,
                TotalMatchingTransactions: { $sum: 1 },
                TotalFraudTransactions: {
                    $sum: { $cond: [{ $eq: ["$Fraud_Label", "Fraud"] }, 1, 0] }
                    }
                }
            },
        {
            $project: {
                _id: 0,
                TotalMatchingTransactions: 1,
                FraudRate: {
                    $multiply: [{ $divide: ["$TotalFraudTransactions", "$TotalMatchingTransactions"] }, 100]
                    }
                }
            }
        ])


    //  2.2.2 Identifiez les transactions effectuées à une heure inhabituelle (Unusual_Time_Transaction)
    //ET à plus de 100 km du domicile du client (Distance_From_Home). Parmi ces transactions, combien sont
    //frauduleuses ?

db.transactions.aggregate([
    {
        $match: {
            "Distance_From_Home": { $gt: 100},
            "Unusual_Time_Transaction": true
        }
    },
    {
        $group: {
            _id: null,
            TotalTransactions: {$sum: 1},
            TotalFraud: {$sum: {$cond: [{$eq: ["$Fraud_Label", "Fraud"]}, 1, 0]}}
        }
    },
    {
        $project: {
            _id: 0
        }
    }
])

    // 2.2.3 Recherchez les transactions effectuées dans les catégories de marchands suivantes :
    //"Electronics", "Clothing", "Restaurant". Affichez uniquement le Transaction_ID, le montant, la catégorie et
    //le statut de fraude.

    db.transactions.aggregate([
        {
            $match: {
                "Merchant_Category": { $in: ["Electronics", "Clothing", "Restaurant"] }
            }
        },
        {
            $project:{
                _id: 0,
                "Transaction_ID": 1,
                "Transaction_Amount (in Million)": 1,
                "Merchant_Category": 1,
                "Fraud_Label": 1
            }
        }
        ])

    // 2.3.1 Le système a détecté une erreur : toutes les transactions du client "24239" datant
    //du 15/01/2025 ont été incorrectement marquées comme frauduleuses. Corrigez cette erreur en les
    //marquant comme légitimes.

    db.transactions.find({Customer_ID: 24239,
        Transaction_Date: { $gte: new Date("2025-01-15") }})

    db.transactions.updateMany({
        Customer_ID: 24239,
        Transaction_Date: { $gte: new Date("2025-01-15") },
        Fraud_Label: "Fraud"
        },
        {
            $set: { "Fraud_Label": "Normal" }
            })

    //  2.3.2 Ajoutez un nouveau champ risk_level à toutes les transactions. Ce champ doit être
    //calculé selon ces règles :
    //"HIGH" si : (montant > 10 millions) OU (Previous_Fraud_Count > 2) OU (Distance_From_Home >
    //500)
    //"MEDIUM" si : (montant > 5 millions) OU (Transaction internationale) OU (Failed_Transaction_Count
    //> 3)
    //"LOW" pour tous les autres cas
    //Implémentez cette logique en plusieurs étapes de mise à jour.

    db.transactions.updateMany(
        {},
        { $set: { risk_level: "LOW" } }
        )

    db.transactions.updateMany(
        {
            $or: [
                { "Transaction_Amount (in Million)": { $gt: 5 } },
                { "Is_International_Transaction": true },
                { "Failed_Transaction_Count": { $gt: 3 } }
                ]
            },
        { $set: { risk_level: "MEDIUM" } }
        )

    db.transactions.updateMany(
        {
            $or: [
                { "Transaction_Amount (in Million)": { $gt: 10 } },
                { "Previous_Fraud_Count": { $gt: 2 } },
                { "Distance_From_Home": { $gt: 500 } }
                ]
            },
        { $set: { risk_level: "HIGH" } }
        )

//  2.3.3 Pour des raisons de conformité RGPD, vous devez anonymiser les IP_Address de toutes les
    //transactions de plus de 2 ans. Remplacez-les par "ANONYMIZED".
    db.transactions.updateMany(
        {
            Transaction_Date: {
                $lt: new Date("2025-02-01")
                }
            },
        {
            $set: { IP_Address: "ANONYMIZED" }
            }
        )

    //2.4.1 Créez une collection archive_transactions et déplacez-y toutes les transactions
    //frauduleuses ayant plus de 3 tentatives échouées (Failed_Transaction_Count >= 3). Après vérification,
    //supprimez-les de la collection principale
    db.transactions.aggregate([
        {
            $match: {
                Fraud_Label: "Fraud",
                Failed_Transaction_Count: { $gte: 2 }
                }
            },
        {
            $out: "archive_transactions"
            }
        ])

    db.archive_transactions.countDocuments()

    db.transactions.deleteMany({
        Fraud_Label: "Fraud",
        Failed_Transaction_Count: { $gte: 2 }
        })

    db.transactions.countDocuments({
        Fraud_Label: "Fraud",
        Failed_Transaction_Count: { $gte: 2 }
        })