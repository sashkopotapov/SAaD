package ucu.learning;

import static java.lang.String.format;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 *
 */
public class App {
    
    private static final AtomicLong idGen = new AtomicLong(System.currentTimeMillis()/10000);
    
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
    
    public static class BankAccount {
        public final Long id;
        public final String number;
        public final Long owner;
        public final BigDecimal amount;
        
        public BankAccount(final Long id, final String number, final Long owner, final BigDecimal amount) {
            this.id = id;
            this.number = number;
            this.owner = owner;
            this.amount = amount;
        }
        
        @Override 
        public String toString() {
            return String.format("BankAccount ( %s, %s, %s, %s)", id, number, owner, amount);
        }
    }
    
    public static class Transfer {
        public final Long id;
        public final Long fromAccount;
        public final Long toAccount;
        public final BigDecimal amount;
        public final Date transferDate;
        
        public Transfer(final Long id, final Long fromAccount, final Long toAccount, final BigDecimal amount, final Date transferDate) {
            this.id = id;
            this.fromAccount = fromAccount;
            this.toAccount = toAccount;
            this.amount = amount;
            this.transferDate = transferDate;
        }
        
        @Override 
        public String toString() {
            return String.format("Transfer ( %s, %s, %s, %s, %s)", id, fromAccount, toAccount, amount, transferDate);
        }
    }
    
    public static Optional<Long> insertPerson(final String surname, final String name, final Date dob, final Connection conn) {
        final long id = idGen.incrementAndGet();
        System.out.print(id);
        try (final PreparedStatement ps = conn.prepareStatement("insert into person_ (_id, name_, surname_, dob_) values(?, ?, ?, ?);")) {
            ps.setLong(1, id);
            ps.setString(2, surname);
            ps.setString(3, name);
            ps.setDate(4, new java.sql.Date(dob.getTime()));
            if (ps.executeUpdate() != 0) {
                return Optional.of(id);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
        return Optional.of(id);
    }
    
    public static Optional<Long> insertBankAccount(final String number, final Long owner, final BigDecimal amount, final Connection conn) {
        final long id = idGen.incrementAndGet();
        System.out.print(id);
        try (final PreparedStatement ps = conn.prepareStatement("insert into bankaccount_ (_id, number_, owner_, amount_) values(?, ?, ?, ?);")) {
            ps.setLong(1, id);
            ps.setString(2, number);
            ps.setLong(3, owner);
            ps.setBigDecimal(4, amount);
            if (ps.executeUpdate() != 0) {
                return Optional.of(id);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
        return Optional.of(id);
    }
    
    public static Optional<Long> insertTransfer(final Long fromAccount, final Long toAccount, final BigDecimal amount, final Date transferDate, final Connection conn) {
        final long id = idGen.incrementAndGet();
        
        try (final PreparedStatement ps = conn.prepareStatement("insert into transfer_ (_id, fromaccount_, toaccount_, amount_, transferdate_) values(?, ?, ?, ?, ?);")) {
            ps.setLong(1, id);
            ps.setLong(2, fromAccount);
            ps.setLong(3, toAccount);
            ps.setBigDecimal(4, amount);
            ps.setDate(5, new java.sql.Date(transferDate.getTime()));
            if (ps.executeUpdate() != 0) {
                return Optional.of(id);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
        return Optional.of(id);
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
    
    public static List<BankAccount> allBankAccount(final Connection conn) {
        final var bankAccounts = new ArrayList<BankAccount>();
        try (final Statement st = conn.createStatement(); 
             final ResultSet rs = st.executeQuery("select * from bankaccount_")) {
            while (rs.next()) {
                var ba = new BankAccount(rs.getLong("_id"), rs.getString("number_"), rs.getLong("owner_"), rs.getBigDecimal("amount_"));
                bankAccounts.add(ba);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return bankAccounts;
    } 
    
    public static List<Transfer> allTransfers(final Connection conn) {
        final var transfers = new ArrayList<Transfer>();
        try (final Statement st = conn.createStatement(); 
             final ResultSet rs = st.executeQuery("select * from transfer_")) {
            while (rs.next()) {
                var ba = new Transfer(rs.getLong("_id"), rs.getLong("fromaccount_"), rs.getLong("toaccount_"), rs.getBigDecimal("amount_"), rs.getTimestamp("transferdate_"));
                transfers.add(ba);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return transfers;
    }
    
    public static int updateAmountOnAccount(final Long account, final BigDecimal amount, final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("update bankaccount_ set amount_ = ? where _id = ?")) {
            ps.setBigDecimal(1, amount);
            ps.setLong(2, account);
            return ps.executeUpdate();
        } catch (final Exception ex) {
            ex.printStackTrace();
            return 0;
        }
    }
    
    public static Optional<BigDecimal> bankAccountAmount(final Long account, final Connection conn) {
        try (final PreparedStatement  ps = conn.prepareStatement("select amount_ from bankaccount_ where _id = ?")) {
            ps.setLong(1, account);
            try (final ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getBigDecimal("amount_")) : Optional.empty();
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
    }
    
    public static Optional<Exception> transfer(final Long fromAccount, final Long toAccount, final BigDecimal amount, final Connection conn) {
        try {
            conn.setAutoCommit(false);
            
            final Optional<BigDecimal> opFromAmount = bankAccountAmount(fromAccount, conn);
            if (opFromAmount.isPresent()) {
                final BigDecimal fromAmount = opFromAmount.get();
                updateAmountOnAccount(fromAccount, fromAmount.subtract(amount), conn);
            }
            
            final Optional<BigDecimal> opToAmount = bankAccountAmount(toAccount, conn);
            if (opToAmount.isPresent()) {
                final BigDecimal toAmount = opToAmount.get();
                updateAmountOnAccount(toAccount, toAmount.add(amount), conn);
            }
            
            final Optional<Long> opTransferId = insertTransfer(fromAccount, toAccount, amount, new Date(), conn);
            if (opTransferId.isPresent() && opFromAmount.isPresent() && opToAmount.isPresent()) {
               conn.commit(); 
            } else {
                conn.rollback();
            }
            conn.commit();
        } catch(final Exception ex) {
            try {
                conn.rollback();
            } catch (final Exception e) {
                e.printStackTrace();
            }
            ex.printStackTrace();
            return Optional.of(ex);
        }
        return Optional.empty();
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
    
    public static int deleteAllPersonnel(final Connection conn) {
        deleteAllBankAccounts(conn);
        try (final PreparedStatement ps = conn.prepareStatement("delete from person_")) {
            return ps.executeUpdate();
        } catch (final Exception ex) {
           ex.printStackTrace();
           return 0;
        }
    }
    
    public static int deleteAllBankAccounts(final Connection conn) {
        deleteAllTransfers(conn);
        try (final PreparedStatement ps = conn.prepareStatement("delete from bankaccount_")) {
            return ps.executeUpdate();
        } catch (final Exception ex) {
           ex.printStackTrace();
           return 0;
        }
    }
    
    public static int deleteAllTransfers(final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("delete from transfer_")) {
            return ps.executeUpdate();
        } catch (final Exception ex) {
           ex.printStackTrace();
           return 0;
        }
    }
    
    public static int deleteAllTransfersForAccount(final Long account, final Connection conn) {
        try (final PreparedStatement ps = conn.prepareStatement("delete from transfer_ where _id = ?")) {
            ps.setLong(1, account);
            return ps.executeUpdate();
        } catch (final Exception ex) {
           ex.printStackTrace();
           return 0;
        }
    }
    
    public static void printId(final Long id) {
        System.out.println("ID" + id);
    }
    
    public static Optional<Date> mkDate(final int year, final int month, final int day) {
        final String pattern = "yyyy-MM-dd";
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        try {
            return Optional.of(simpleDateFormat.parse(format("%s-%s-%s", year, month, day)));
        } catch(ParseException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String dbUrl = "jdbc:postgresql://localhost:5432/foundation";
        final Optional<Date> dob1 = mkDate(2020, 10, 9);
        try (final Connection conn = DriverManager.getConnection(dbUrl, "dbuser", "dbpassw0rd")) {
            System.out.printf("The number of personnel deleted: %s%n", deleteAllPersonnel(conn));
            final Optional<Long> firstPersonId = insertPerson("Potapov", "Sashko", new Date(5 - 8 - 1998), conn);
            final Optional<Long> secondPersonId = insertPerson("Potapova", "Sashko", new Date(5 - 8 - 1996), conn);
            allPersons(conn).forEach(System.out::println);
            
            if (firstPersonId.isPresent() && secondPersonId.isPresent()) {
                final Optional<Long> firstBankAccount = insertBankAccount("10000300", firstPersonId.get(), new BigDecimal("25.00"), conn);
                final Optional<Long> secondBankAccount = insertBankAccount("10000300", secondPersonId.get(), new BigDecimal("25.00"), conn);
                allBankAccount(conn).forEach(System.out::println);
                
                if (firstBankAccount.isPresent() && secondBankAccount.isPresent()) {
                    transfer(firstBankAccount.get(), secondBankAccount.get(), new BigDecimal("5.00"), conn);
                    allTransfers(conn).forEach(System.out::println);
                }
            }        
        }
    }
}
