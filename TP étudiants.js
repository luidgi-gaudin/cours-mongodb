
    // 1. Créer la base et la collection "etudiants"
    use tp_mongodb
    db.createCollection("etudiants")

    // 2. Insérer 5 étudiants avec nom, prénom, âge, ville
    db.etudiants.insertMany([
        { nom: "Dupont", prenom: "Lucas", age: 22, ville: "Paris" },
        { nom: "Martin", prenom: "Emma", age: 19, ville: "Lyon" },
        { nom: "Bernard", prenom: "Hugo", age: 21, ville: "Marseille" },
        { nom: "Petit", prenom: "Léa", age: 18, ville: "Paris" },
        { nom: "Roux", prenom: "Antoine", age: 23, ville: "Toulouse" }
        ])

    // 3. Afficher tous les étudiants
    db.etudiants.find()

    // 4. Trouver les étudiants de plus de 20 ans
    db.etudiants.find({ age: { $gt: 20 } })

    // 5. Modifier la ville d'un étudiant
    db.etudiants.updateOne(
        { nom: "Martin", prenom: "Emma" },
        { $set: { ville: "Bordeaux" } }
        )

    // 6. Ajouter un champ "notes" (tableau) à tous les étudiants
    db.etudiants.updateMany(
        {},
        { $set: { notes: [] } }
        )

    // 6 bis. Exemple : ajouter une note à un étudiant
    db.etudiants.updateOne(
        { nom: "Dupont" },
        { $push: { notes: { matiere: "Maths", note: 15 } } }
        )

    // 7. Supprimer les étudiants d'une ville spécifique (Paris)
    db.etudiants.deleteMany({ ville: "Paris" })

    // 8. Compter le nombre d'étudiants restants
    db.etudiants.countDocuments()