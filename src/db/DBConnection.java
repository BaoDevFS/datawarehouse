package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DBConnection {
	static Connection con;
	static String WAREHOUSE = "jdbc:mysql://localhost:3306/datawarehouse?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
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

//		try {
//			if (dbname.equals("STAGING")) {
//				conn = DriverManager.getConnection(STAGING, username, password);
//
//			} else if (dbname.equals("WAREHOUSE")) {
//				conn = DriverManager.getConnection(WAREHOUSE, username, password);
//			} else {
//				conn = DriverManager.getConnection(CONTROLDB, username, password);
//			}
//			return conn;
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			return null;
//		}
		// kiểm tra connection bằng null hay bị đóng kết nối thì tạo lại
		try {
			if (con == null || con.isClosed()) {
				new DBConnection(dbname);
				return con;
			} else {
				String url = con.getMetaData().getURL();
				// kiểm tra cái DB muốn lấy và DB hiện tại có giống nhau
				// giống thì trả về con
				// khác thì new mới connection theo dbname
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
			e.printStackTrace();
			return null;
		}

	}

	public static void main(String[] args) throws SQLException {

		try {
			Connection con1 = DBConnection.getConnection("CONTROLDB");
			Connection con2 = DBConnection.getConnection("STAGING");
			String sql = "Select * from config";
			PreparedStatement pre = con1.prepareStatement(sql);
			ResultSet rs = pre.executeQuery();
			//
			String sql2 = "Select * from staging";
			PreparedStatement pre2 = con2.prepareStatement(sql2);
			ResultSet rs2 = pre2.executeQuery();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
