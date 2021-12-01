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

  public static String[] PREP_SELECT = {
      "SELECT age, Count(id) FROM tbl WHERE age = ? GROUP BY age",
      "SELECT age, name, Count(id) FROM tbl GROUP BY age, name HAVING name = ? AND age = ?",
      "SELECT * FROM tbl WHERE id = ?",
      "SELECT * FROM tbl WHERE id = ? AND age > ?"
  };

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Connection connection = DriverManager.getConnection(JDBC_STRING);
    Statement stmt = connection.createStatement();
    stmt.execute(CREATE_TABLE);

    PreparedStatement prepInsert = connection.prepareStatement(PREP_INSERT);
    prepInsert.setInt(1, 1);
    prepInsert.setString(2, "John");
    prepInsert.setInt(3, 25);
    assert prepInsert.executeUpdate() == 1;
    prepInsert.setInt(1, 2);
    prepInsert.setString(2, "Smith");
    prepInsert.setInt(3, 35);
    assert prepInsert.executeUpdate() == 1;

    PreparedStatement prepSelect = connection.prepareStatement(PREP_SELECT[0]);
    prepSelect.setInt(1, 25);
    ResultSet resultSet = prepSelect.executeQuery();
    ResultSetMetaData metadata = resultSet.getMetaData();
    assert metadata.getColumnName(1).equals("age");
    assert metadata.getColumnName(2).equals("Count");
    assert resultSet.next();
    assert resultSet.getInt(1) == 25;
    assert resultSet.getInt(2) == 1;
    assert !resultSet.next();

    prepSelect = connection.prepareStatement(PREP_SELECT[1]);
    prepSelect.setString(1, "Smith");
    prepSelect.setInt(2, 35);
    resultSet = prepSelect.executeQuery();
    metadata = resultSet.getMetaData();
    assert metadata.getColumnName(1).equals("age");
    assert metadata.getColumnName(2).equals("name");
    assert metadata.getColumnName(3).equals("Count");
    assert resultSet.next();
    assert resultSet.getInt(1) == 35;
    assert resultSet.getString(2).equals("Smith");
    assert resultSet.getInt(3) == 1;
    assert !resultSet.next();

    prepSelect = connection.prepareStatement(PREP_SELECT[2]);
    prepSelect.setInt(1, 1);
    resultSet = prepSelect.executeQuery();
    metadata = resultSet.getMetaData();
    assert metadata.getColumnName(1).equals("id");
    assert metadata.getColumnName(2).equals("name");
    assert metadata.getColumnName(3).equals("age");
    assert resultSet.next();
    assert resultSet.getInt(1) == 1;
    assert resultSet.getString(2).equals("John");
    assert resultSet.getInt(3) == 25;
    assert !resultSet.next();

    prepSelect = connection.prepareStatement(PREP_SELECT[3]);
    prepSelect.setInt(1, 2);
    prepSelect.setInt(2, 30);
    resultSet = prepSelect.executeQuery();
    metadata = resultSet.getMetaData();
    assert metadata.getColumnName(1).equals("id");
    assert metadata.getColumnName(2).equals("name");
    assert metadata.getColumnName(3).equals("age");
    assert resultSet.next();
    assert resultSet.getInt(1) == 2;
    assert resultSet.getString(2).equals("Smith");
    assert resultSet.getInt(3) == 35;
    assert !resultSet.next();

    connection.close();
  }
}