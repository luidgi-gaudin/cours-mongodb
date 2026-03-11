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
            $group: {
                _id: null,
                TotalMatchingTransactions: {
                    $sum: {
                        $cond: [
                            {
                                $and: [
                                    {$gt: ["$Transaction_Amount (in Million)", 5]},
                                    {$eq: ["$Is_International_Transaction", true]},
                                    {$eq: ["$Card_Type", "Credit"]},
                                    {$gt: ["$Previous_Fraud_Count", 0]}
                                    ]
                                },
                            1,
                            0
                            ]
                        }
                    },
                TotalFraudTransactions: {
                    $sum: {
                        $cond: [
                            {
                                $and: [
                                    {$gt: ["$Transaction_Amount (in Million)", 5]},
                                    {$eq: ["$Is_International_Transaction", true]},
                                    {$eq: ["$Card_Type", "Credit"]},
                                    {$gt: ["$Previous_Fraud_Count", 0]},
                                    {$eq: ["$Fraud_Label", "Fraud"]}
                                    ]
                                },
                            1,
                            0
                            ]
                        }
                    }
                }
            },
        {
            $project: {
                _id: 0,
                TotalMatchingTransactions: 1,
                FraudRate: {$multiply: [{$divide: ["$TotalFraudTransactions", "$TotalMatchingTransactions"]}, 100]}
                }
            }
        ])