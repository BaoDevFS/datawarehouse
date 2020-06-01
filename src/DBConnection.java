
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	public static Connection getConnection(String jdbcURL, String userName, String password)
			throws ClassNotFoundException, SQLException {

		Connection connection = DriverManager.getConnection(jdbcURL, userName, password);

		return connection;

	}
}
