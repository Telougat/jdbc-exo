import java.sql.*;

public class TestLorenzo implements DataBase {

    Connection conn = null;
    Statement st = null;
    ResultSet rs = null;

    public TestLorenzo() {
        conn = this.Connection("db.properties");
        try {
            conn.setAutoCommit(false);
        }
        catch (Exception e) { System.out.println(e.getMessage()); }
    }

    public static void main(String[] args) {
        TestLorenzo bdd = new TestLorenzo();
        //bdd.Select("*", "testlorenzo");
        //bdd.CreateTable("test10", "id INT PRIMARY KEY NOT NULL, first_name VARCHAR(40) NOT NULL, last_name VARCHAR(40) NOT NULL, dobDATE DATE NOT NULL");
        //bdd.insertInto("Timothé", "Ceci est un test", new Date(System.currentTimeMillis()));
        bdd.Select("*", "testlorenzo");
        bdd.Disconnect();
    }



    public boolean Select(String row, String table) {
        try {
            PreparedStatement ps = conn.prepareStatement("SELECT " + row + " FROM " + table, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            rs = ps.executeQuery();

            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            while (rs.next()) {
                int id = rs.getInt("ID");
                String author = rs.getString("Author");
                String comment = rs.getString("Comment");
                Date date = rs.getDate("Time");

                if (author.equals("Lorenzo2")) {
                    rs.updateString("Author", "LorenzoTest");
                    rs.updateRow();
                }

                System.out.println("Id : " + id + " Author : " + author + " Comment : " + comment + " Date : " + date);
            }
            conn.commit();
        }
        catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return true;
    }

    public boolean deleteWhereId(String table, Integer id) {
        try {
            PreparedStatement ps = conn.prepareStatement("DELETE FROM " + table + " WHERE ID = ?");
            ps.setInt(1, id);
            if (ps.executeUpdate() == -1) {
                conn.rollback();
                return false;
            }

            conn.commit();
        } catch (SQLException ex) {
            try {
                conn.rollback();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return true;
    }

    public boolean deleteAll(String table)
    {
        try {
            PreparedStatement ps = conn.prepareStatement("DELETE FROM " + table);
            if (ps.executeUpdate() == -1) {
                conn.rollback();
                return false;
            }
            conn.commit();
        } catch (SQLException ex) {
            try {
                conn.rollback();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return true;
    }

    public boolean updateWhereId(String author, String comment, Date date, Integer id) {
        try {
            PreparedStatement ps = conn.prepareStatement("UPDATE testlorenzo SET Author = ?, Comment = ?, Time = ? WHERE ID = ?");
            ps.setString(1, author);
            ps.setString(2, comment);
            ps.setDate(3, date);
            ps.setInt(4, id);
            if (ps.executeUpdate()==-1) {
                conn.rollback();
                return false;
            }

            conn.commit();
        } catch (SQLException ex) {
            try {
                conn.rollback();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return true;
    }

    public boolean insertInto(String author, String comment, Date date) {
        try {
            PreparedStatement ps = conn.prepareStatement("INSERT INTO testlorenzo (Author, Comment, Time) VALUES (? , ? , ?)");
            ps.setString(1, author);
            ps.setString(2, comment);
            ps.setDate(3, date);
            if (ps.executeUpdate()==-1) {
                conn.rollback();
                return false;
            }

            conn.commit();
        } catch (SQLException ex) {
            try {
                conn.rollback();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return true;
    }

    public void Disconnect() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException ex) {
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ex) {
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        }
    }
}
