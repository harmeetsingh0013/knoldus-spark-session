package week3.homework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class InsertData {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/spark_stream_receiver";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "root";

    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        Statement stmt = null;

        try {
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            System.out.println("Inserting records into the table...");
            stmt = conn.createStatement();

            for (int i=1; i<=100000; i++) {
                System.out.println("counter : "+i);
                String sql = "INSERT INTO user " +
                        "(uuid, name) " +
                        "VALUES ('"+ UUID.randomUUID()+"', 'spark_stream_"+i+"')";
                stmt.executeUpdate(sql);
            }

        } catch (Exception ex){
            ex.printStackTrace();
        }
        finally {
            stmt.close();
            conn.close();

        }
    }
}
