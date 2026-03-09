use tp_films

    // 1. films entre 2010 et 2020
    db.films.find({annee: {$gt: 2010 ,$lt: 2020} })

    // 2. films de genre "Action" ou "Sci-Fi"
    db.films.find({genres: {$in: ["Action", "Sci-Fi"]} })

    //3. films avec une note >= 8 et au moins 100 000 votes
    db.films.find({note: {$gte: 8}, votes: {$gte: 100000} })

    // 4. films avec Leonardo DiCaprio dans acteurs
    db.films.find({acteurs: {$in: ["Leonardo DiCaprio"] }})

    // 5.afficher uniquement année et titre des films sans id
    db.films.find({}, {titre: 1, annee: 1, _id:0})

    // 6. films dont nom commence par "The"
    db.films.find({titre: /^The/})

    // 7. Lister tout les genres de films sans doublons
    db.films.distinct("genres")

    //8. Afficher les 10 films les mieux noté du meilleurs au pire
    db.films.find().sort({note: -1}).limit(10)

