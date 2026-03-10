// TP1 - Partie 1 : Import et Validation des Données (FraudShield Banking Data)

    // === 1.1 IMPORT DES DONNÉES ===

    // Q1.1.1 - Création/sélection de la base de données
    use fraudshield_banking


    // === 1.2 VALIDATION DE L'IMPORT ===

    // Q1.2.1a - Affiche les 5 premières transactions
    db.transactions.find().limit(5)

    // Q1.2.1b - Compte le nombre total de documents importés
    db.transactions.countDocuments()

    // Q1.2.1c - Affiche la structure d'un document
    db.transactions.findOne()

    // Q1.2.2 - Conversion Yes/No → booléens pour les 3 champs concernés

    // Is_International_Transaction : Yes → true
    db.transactions.updateMany(
        { Is_International_Transaction: "Yes" },
        { $set: { Is_International_Transaction: true } }
        )
    // Is_International_Transaction : No → false
    db.transactions.updateMany(
        { Is_International_Transaction: "No" },
        { $set: { Is_International_Transaction: false } }
        )

    // Is_New_Merchant : Yes → true
    db.transactions.updateMany(
        { Is_New_Merchant: "Yes" },
        { $set: { Is_New_Merchant: true } }
        )
    // Is_New_Merchant : No → false
    db.transactions.updateMany(
        { Is_New_Merchant: "No" },
        { $set: { Is_New_Merchant: false } }
        )

    // Unusual_Time_Transaction : Yes → true
    db.transactions.updateMany(
        { Unusual_Time_Transaction: "Yes" },
        { $set: { Unusual_Time_Transaction: true } }
        )
    // Unusual_Time_Transaction : No → false
    db.transactions.updateMany(
        { Unusual_Time_Transaction: "No" },
        { $set: { Unusual_Time_Transaction: false } }
        )

    // Vérification : affiche les 3 champs convertis
    db.transactions.findOne({}, {
        Is_International_Transaction: 1,
        Is_New_Merchant: 1,
        Unusual_Time_Transaction: 1,
        _id: 0
        })

    // Vérification : compte les documents encore avec Yes/No (résultat attendu: 0)
    db.transactions.countDocuments({
        $or: [
            { Is_International_Transaction: { $in: ["Yes", "No"] } },
            { Is_New_Merchant: { $in: ["Yes", "No"] } },
            { Unusual_Time_Transaction: { $in: ["Yes", "No"] } }
            ]
        })
