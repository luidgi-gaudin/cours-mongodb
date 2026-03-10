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