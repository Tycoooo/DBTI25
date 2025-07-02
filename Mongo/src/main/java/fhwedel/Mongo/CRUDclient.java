package fhwedel.Mongo;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.*;
import org.bson.Document;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Updates;

public class CRUDclient {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db = mongoClient.getDatabase("bibliothek");
        MongoCollection<Document> books = db.getCollection("buch");
        MongoCollection<Document> readers = db.getCollection("leser");
        MongoCollection<Document> loans = db.getCollection("entliehen");

        //Marc-Uwe hinzufügen
        books.insertOne(new Document("invr", 1)
                .append("autor", "Marc-Uwe Kling")
                .append("titel", "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers")
                .append("verlag", "Ulstein-Verlag"));

        //Friedrich Funke hinzufügen
        readers.insertOne(new Document("lnr", 1)
                .append("name", "Friedrich Funke")
                .append("adresse", "Bahnhofstraße 17, 23758 Oldenburg"));

        //5 Leser hinzufügen
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
        readers.insertMany(readerDocs);

        //5 Bücher hinzufügen
        List<Document> booksDocs = Arrays.asList(
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

        //Person leiht Buch 
        List<Document> loansDocs = Arrays.asList(
                new Document(Map.of(
                       "lnr", 2,
                        "invr", 2,
                        "returnDate", new GregorianCalendar(2025, Calendar.JULY, 2).getTime()
                )),
                new Document(Map.of(
                        "lnr", 2,
                        "invr", 3,
                        "returnDate", new GregorianCalendar(2026, Calendar.JUNE, 5).getTime()
                )),
                new Document(Map.of(
                        "lnr", 4,
                        "invr", 5,
                        "returnDate", new GregorianCalendar(2026, Calendar.JUNE, 5).getTime()
                )),
                new Document(Map.of(
                        "lnr", 4,
                        "invr", 4,
                        "returnDate", new GregorianCalendar(2026, Calendar.JUNE, 5).getTime()
                ))
                
        );
        loans.insertMany(loansDocs);

        Document result_2b = books.find(Filters.eq("autor", "Marc-Uwe Kling")).first();

        System.out.println("2b) Bücher von Marc-Uwe Kling: \"" + result_2b.getString("titel") + '"');
        System.out.println("2c) Anzahl an Büchern in Bibliothek: " + books.countDocuments());

        AggregateIterable<Document> result_2d = loans.aggregate(Arrays.asList(
                    Aggregates.group("$lnr", Accumulators.sum("anzahlBuecher", 1)),
                    Aggregates.match(Filters.gt("anzahlBuecher", 1)),
                    Aggregates.sort(Sorts.descending("anzahlBuecher"))
                ));

        // Ausgabe 2d
        System.out.println("2d) Namen der Leser mit mindestens 2 ausgeliehenen Büchern:");
        for (Document doc : result_2d) {
                String name = readers.find(Filters.eq("lnr", doc.get("_id"))).first().getString("name");
                Integer numberOfBooks = doc.getInteger("anzahlBuecher");
                System.out.println("    Leser: " + name + ", Bücher ausgeliehen: " + numberOfBooks);
        }

        //2e Friedrich Funke leiht Känguru-Chroniken aus
        loans.insertOne(
                new Document(Map.of("lnr", 1, "invr", 1, "returnDate", new GregorianCalendar(2026, Calendar.JUNE, 5).getTime()))
        );

        //2e Friedrich Funke gibt Känguru-Chroniken zurück
        loans.deleteOne(Filters.and(
                Filters.eq("lnr", 1),
                Filters.eq("invr", 1)
        ));

        //2f Heinz Müller leiht 2 Bücher aus
        /*
        Vorteile:
                - schneller Zugriff (alles in einem Dokument, kein Join notwendig)
                - einfache Struktur (nicht so stark verschachtelt)
        Nachteile:
                - schwere Wartung (bei Änderung des Verlags muss dieser überall geändert werden)
                - schwierige Abfrage (von wem wird bestimmtes Buch ausgeliehen -> alle Leser durchgehen)
        */
        Document heinzMueller = new Document(Map.of(
                "lnr", 99,
                "name", "Heinz Müller",
                "adresse", "Klopstockweg 17, 38124 Braunschweig",
                "loans", Arrays.asList(
                        new Document(Map.of(
                                "invr", 1,
                                "titel", "Die Känguru-Chroniken",
                                "autor", "Marc-Uwe Kling",
                                "verlag", "Ulstein-Verlag"
                        )),
                        new Document(Map.of(
                                "invr", 7,
                                "titel", "Der König von Berlin",
                                "autor", "Horst Evers",
                                "verlag", "Rowohlt-Verlag"
                        ))
                )
        ));

        readers.insertOne(heinzMueller);

        //2g Heinz Müller gibt Känguru-Chroniken zurück
        readers.updateOne(
            Filters.eq("lnr", 99),
            Updates.pull("loans", new Document("invr", "1"))
        );

        //2g Friedrich Funke leiht Känguru-Chroniken aus
        readers.updateOne(
            Filters.eq("lnr", 1),
            Updates.push("loans", new Document()
                    .append("invr", 1)
                    .append("titel", "Die Känguru-Chroniken")
                    .append("autor", "Marc-Uwe Kling")
                    .append("verlag", "Ulstein-Verlag"))
        );

        books.drop();
        readers.drop();
        loans.drop();

        db = mongoClient.getDatabase("firma");
        MongoCollection<Document> personal = db.getCollection("personal");
        MongoCollection<Document> gehalt = db.getCollection("gehalt");
        MongoCollection<Document> abteilung = db.getCollection("abteilung");
        MongoCollection<Document> kind = db.getCollection("kind");
        MongoCollection<Document> maschine = db.getCollection("maschine");
        MongoCollection<Document> praemie = db.getCollection("praemie");


        mongoClient.close();
        System.exit(0);
    }
}