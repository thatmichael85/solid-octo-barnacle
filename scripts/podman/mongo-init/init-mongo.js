db = new Mongo().getDB("DefaultDatabase");
db.yourCollection.insert({ exampleField: 'exampleValue' });