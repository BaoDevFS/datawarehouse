package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	static Connection con;
	static String WAREHOUSE = "jdbc:mysql://localhost:3306/wasehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	static String CONTROLDB = "jdbc:mysql://localhost/controldb?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	static String STAGING = "jdbc:mysql://localhost/staging?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	static String username = "root";
	static String password = "";

	private DBConnection(String dbname) {
		try {
			if (dbname.equals("CONTROLDB")) {

				con = DriverManager.getConnection(CONTROLDB, username, password);

			} else if (dbname.equals("WAREHOUSE")) {
				con = DriverManager.getConnection(WAREHOUSE, username, password);
			} else {
				con = DriverManager.getConnection(STAGING, username, password);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Connection getConnection(String dbname) {
		try {
			
			if (con == null||con.isClosed()) {
				new DBConnection(dbname);
				return con;
			} else {
				 String url = con.getMetaData().getURL();

				if (dbname.equals("CONTROLDB") && CONTROLDB.equals(url)) {
					return con;
				} else if (dbname.equals("WAREHOUSE") && WAREHOUSE.equals(url)) {
					return con;
				} else if (dbname.equals("STAGING") && STAGING.equals(url)) {
					return con;
				} else {
					new DBConnection(dbname);
					return con;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return con;
		}
	}

	public static void main(String[] args) throws SQLException {
		DBConnection.getConnection("CONTROLDB");
		DBConnection.getConnection("CONTROLDB");
	}

}
