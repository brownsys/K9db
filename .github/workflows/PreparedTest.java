import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

class PreparedTest {
  public static String JDBC_STRING =
      "jdbc:mariadb://127.0.0.1:10001?user=root&password=password&"
      + "autoReconnect=true&useSSL=false&sslMode=DISABLED&useServerPrepStmts"
      + "&verifyServerCertificate=false";

  public static String CREATE_TABLE =
      "CREATE TABLE tbl("
      + "id int,"
      + "name varchar(100),"
      + "age int"
      + ")";

  public static String PREP_INSERT =
      "INSERT INTO tbl VALUES(?, ?, ?);";
  
  public static String PREP_SELECT =
       "SELECT age, Count(id) FROM tbl WHERE age = ? GROUP BY age";

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Connection connection = DriverManager.getConnection(JDBC_STRING);
    Statement stmt = connection.createStatement();
    stmt.execute(CREATE_TABLE);

    PreparedStatement prepInsert = connection.prepareStatement(PREP_INSERT);        
    prepInsert.setInt(1, 1);
    prepInsert.setString(2, "John");
    prepInsert.setInt(3, 25);
    assert prepInsert.executeUpdate() == 1;

    PreparedStatement prepSelect = connection.prepareStatement(PREP_SELECT);
    prepSelect.setInt(1, 25);
    ResultSet resultSet = prepSelect.executeQuery();

    ResultSetMetaData metadata = resultSet.getMetaData();
    int columnsNumber = metadata.getColumnCount();
    
    assert metadata.getColumnName(1).equals("age");
    assert metadata.getColumnName(2).equals("Count");
    assert resultSet.next();
    assert resultSet.getString(1).equals("25");
    assert resultSet.getString(2).equals("1");
    assert !resultSet.next();

    connection.close();
  }
}
