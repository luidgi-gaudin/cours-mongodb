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