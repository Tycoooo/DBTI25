package fhwedel.Mongo;

public class CRUDclient {
  public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
    MongoDatabase db = mongoClient.getDatabase("bibliothek");
    MongoCollection<Document> books = db.getCollection("buch");
    MongoCollection<Document> reader = db.getCollection("leser");
    MongoCollection<Document> loans = db.getCollection("entliehen");
 
    books.insertOne(new Document("invr", 2)
        .append("autor", "Marc-Uwe Kling")
        .append("titel", "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers")
        .append("verlag", "Ulstein-Verlag"));
 
    reader.insertOne(new Document("lnr", 1)
        .append("name", "Friedrich Funke")
        .append("adresse", "Bahnhofstraße 17, 23758 Oldenburg"));
 
    List<Document> readerDocs = Arrays.asList(
        new Document(Map.of(
            "lnr", 2,
            "name", "Marty Menge",
            "adresse", "geheim")),
        new Document(Map.of(
            "lnr", 3,
            "name", "Max Mustermann",
            "adresse", "12345 Berlin, Große Straße 1")),
        new Document(Map.of(
            "lnr", 4,
            "name", "Bob der Baumeister",
            "adresse", "in Bobs Welt")),
        new Document(Map.of(
            "lnr", 5,
            "name", "Hildegard Müller",
            "adresse", "überall")),
        new Document(Map.of(
            "lnr", 6,
            "name", "Naruto Uzumaki",
            "adresse", "Konoha")));
    reader.insertMany(readerDocs);
 
    List<Document> booksDocs = Arrays.asList(
        new Document(Map.of(
            "invr", 1,
            "autor", "Marc-Uwe Kling",
            "titel", "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers",
            "verlag", "Ulstein-Verlag")),
        new Document(Map.of(
            "invr", 2,
            "autor", "Hajime Isayama",
            "titel", "Attack on Titan",
            "verlag", "Kōdansha")),
        new Document(Map.of(
            "invr", 3,
            "autor", "Max Mustermann",
            "titel", "Buch",
            "verlag", "Verlag")),
        new Document(Map.of(
            "invr", 4,
            "autor", "Hendrik Hengst",
            "titel", "haarige Hühner",
            "verlag", "Verlag")),
        new Document(Map.of(
            "invr", 5,
            "autor", "Großer Gerd",
            "titel", "Gerds Autobiografie",
            "verlag", "Verlag")),
        new Document(Map.of(
            "invr", 6,
            "autor", "Turbo Torsten",
            "titel", "Turbo Traktor",
            "verlag", "Turbo Verlag")));
 
    books.insertMany(booksDocs);
 
   
 
    mongoClient.close();
    }
}
