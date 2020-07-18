package part2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	static String jdbcURL_1 = "jdbc:mysql://localhost/staging?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	static String jdbcURL_2 = "jdbc:mysql://localhost/controldb?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	static String userName_1 = "root";
	static String password_1 = "";

	public static Connection getConnection(String url) throws ClassNotFoundException, SQLException {
		Connection connection = DriverManager.getConnection(url, userName_1, password_1);
		return connection;
	}
}