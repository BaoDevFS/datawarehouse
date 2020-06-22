package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	static String jdbcURL_1 = "jdbc:mysql://localhost/datawarehouse?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	static String userName_1 = "root";
	static String password_1 = "";

	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		Connection connection = DriverManager.getConnection(jdbcURL_1, userName_1, password_1);
		return connection;
	}
}
