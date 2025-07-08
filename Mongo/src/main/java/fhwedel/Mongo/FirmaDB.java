package fhwedel.Mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
 
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
 
public class FirmaDB {
 
    public static void main(String[] args) throws Exception {
        //Verbindung zu MariaDB
        Connection conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306/firma", "root", "password");
 
        // Verbindung zu MongoDB
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase mongoDB = mongoClient.getDatabase("firma");
        MongoCollection<Document> personalCollection = mongoDB.getCollection("personal");
        MongoCollection<Document> maschinenCollection = mongoDB.getCollection("maschinen");
 
        String query = "SELECT * FROM personal";
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery(query);

while (rs.next()) {
    String pnr = rs.getString("pnr");

    Document mitarbeiter = new Document("pnr", pnr)
            .append("name", rs.getString("name"))
            .append("vorname", rs.getString("vorname"))
            .append("krankenkasse", rs.getString("krankenkasse"));

    // Abteilung separat abfragen
    PreparedStatement abtStmt = conn.prepareStatement(
            "SELECT * FROM abteilung WHERE abt_nr = ?");
    abtStmt.setString(1, rs.getString("abt_nr"));
    ResultSet abtRs = abtStmt.executeQuery();
    if (abtRs.next()) {
        Document abteilung = new Document("abt_nr", abtRs.getString("abt_nr"))
                .append("name", abtRs.getString("name"));
        mitarbeiter.append("abteilung", abteilung);
    }
    abtRs.close();
    abtStmt.close();

    // Gehalt separat abfragen
    PreparedStatement gehaltStmt = conn.prepareStatement(
            "SELECT * FROM gehalt WHERE geh_stufe = ?");
    gehaltStmt.setString(1, rs.getString("geh_stufe"));
    ResultSet gehaltRs = gehaltStmt.executeQuery();
    if (gehaltRs.next()) {
        Document gehalt = new Document("geh_stufe", gehaltRs.getString("geh_stufe"))
                .append("betrag", gehaltRs.getDouble("betrag"));
        mitarbeiter.append("gehalt", gehalt);
    }
    gehaltRs.close();
    gehaltStmt.close();

    // Kinder separat abfragen
    PreparedStatement childStmt = conn.prepareStatement(
            "SELECT * FROM kind WHERE pnr = ?");
    childStmt.setString(1, pnr);
    ResultSet kinderRs = childStmt.executeQuery();
    List<Document> kinderList = new ArrayList<>();
    while (kinderRs.next()) {
        Document kind = new Document()
                .append("name", kinderRs.getString("k_name"))
                .append("vorname", kinderRs.getString("k_vorname"))
                .append("geburtstag", kinderRs.getInt("k_geb"));
        kinderList.add(kind);
    }
    mitarbeiter.append("kinder", kinderList);
    kinderRs.close();
    childStmt.close();

    // Prämien separat abfragen
    PreparedStatement praemieStmt = conn.prepareStatement(
            "SELECT * FROM praemie WHERE pnr = ?");
    praemieStmt.setString(1, pnr);
    ResultSet praemieRs = praemieStmt.executeQuery();
    List<Document> praemieList = new ArrayList<>();
    while (praemieRs.next()) {
        Document praemie = new Document()
                .append("pnr", praemieRs.getString("pnr"))
                .append("p_betrag", praemieRs.getDouble("p_betrag"));
        praemieList.add(praemie);
    }
    mitarbeiter.append("praemien", praemieList);
    praemieRs.close();
    praemieStmt.close();

    // Maschinen → separate Collection
    PreparedStatement maschineStmt = conn.prepareStatement(
            "SELECT * FROM maschine WHERE pnr = ?");
    maschineStmt.setString(1, pnr);
    ResultSet maschinenRs = maschineStmt.executeQuery();
    while (maschinenRs.next()) {
        Document maschine = new Document("mnr", maschinenRs.getString("mnr"))
                .append("name", maschinenRs.getString("name"))
                .append("neuwert", maschinenRs.getDouble("neuwert"))
                .append("zeitwert", maschinenRs.getDouble("zeitwert"))
                .append("ansch_datum", maschinenRs.getDate("ansch_datum"))
                .append("pnr", pnr);
        maschinenCollection.insertOne(maschine);
    }
    maschinenRs.close();
    maschineStmt.close();

    // Mitarbeiter-Dokument in die MongoDB speichern
    personalCollection.insertOne(mitarbeiter);
}

rs.close();
stmt.close();
conn.close();
mongoClient.close();

System.out.println("Übertragung abgeschlossen.");

    }

    
}
