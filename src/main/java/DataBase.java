import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public interface DataBase {
    public default Connection Connection(String propertiesFileName) {
        Connection conn = null;
        Properties p = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesFileName);
        try {
            p.load(inputStream);
            conn = DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"));
        }
        catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } catch (IOException io) {
            System.out.println(io.getMessage());
        }
        return conn;
    }
}
