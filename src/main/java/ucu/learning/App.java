package ucu.learning;

import static java.lang.String.format;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 *
 */
public class App {
    
    private static final AtomicLong idGen = new AtomicLong(System.currentTimeMillis()/100000);
    
    public static class Person {
        public final Long id;
        public final String surname;
        public final String name;
        public final Date dob;
        
        public Person(final Long id, final String surname, final String name, final Date dob) {
            this.id = id;
            this.name = name;
            this.surname = surname;
            this.dob = dob;
        } 
        
        @Override 
        public String toString() {
            return String.format("Person ( %s, %s, %s, %s)", id, surname, name, dob);
        }
    }
    
    public static Long insertPerson(final String surname, final String name, final Date dob, final Connection conn) {
        final long id = idGen.incrementAndGet();
        System.out.print(id);
        try (final PreparedStatement ps = conn.prepareStatement("insert into person_ (_id, name_, surname_, dob_) values(?, ?, ?, ?);")) {
            ps.setLong(1, id);
            ps.setString(2, surname);
            ps.setString(3, name);
            ps.setDate(4, (java.sql.Date) dob);
            ps.executeUpdate();
            ps.close();
            conn.commit();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return id;
    }
    
    public static List<Person> allPersons(final Connection conn) {
        final var persons = new ArrayList<Person>();
        try (final Statement st = conn.createStatement(); 
             final ResultSet rs = st.executeQuery("select * from person_")) {
            while (rs.next()) {
                var p = new Person(rs.getLong("_id"), rs.getString("surname_"), rs.getString("name_"), rs.getDate("dob_"));
                persons.add(p);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return persons;
    } 
    
    public static List<Person> findPersonBySurname(final String surname, final Connection conn) {
        final var persons = new ArrayList<Person>();
        try (final PreparedStatement ps = conn.prepareStatement("select * from person_ where surname_ = ?")) {
            ps.setString(1, surname);
            try(final ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    var p = new Person(rs.getLong("_id"), rs.getString("surname_"), rs.getString("name_"), rs.getDate("dob_"));
                    persons.add(p);
                }
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return persons;
    } 
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        final String dbUrl = "jdbc:postgresql://localhost:5432/foundation";
        try (final Connection conn = DriverManager.getConnection(dbUrl, "dbuser", "dbpassw0rd")) {
            conn.setAutoCommit(false);
             findPersonBySurname("Brown", conn).forEach(System.out::println);
             insertPerson("Jones", "Adam", null, conn);
             allPersons(conn).forEach(System.out::println);;
        }
    }
}
