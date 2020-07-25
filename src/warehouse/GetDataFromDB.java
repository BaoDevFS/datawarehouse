package warehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import db.DBConnection;
import mail.SendMail;

public class GetDataFromDB {
	SendMail send;

	public GetDataFromDB() {
		send = new SendMail();
	}

	public ResultSet getDataInConfig(int id) {
		try {
			Connection con = DBConnection.getConnection("CONTROLDB");
			String sql = "Select * from config where id=?";
			PreparedStatement pre = con.prepareStatement(sql);
			pre.setInt(1, id);
			ResultSet rs = pre.executeQuery();
			return rs;
		} catch (SQLException e) {
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối CONTROLDB");
			return null;
		}
	}

	//
	public ResultSet getDataInLogs(int id) {
		try {
			Connection con = DBConnection.getConnection("CONTROLDB");
			String sql = "Select * from logs where id=?";
			PreparedStatement pre = con.prepareStatement(sql);
			pre.setInt(1, id);
			ResultSet rs = pre.executeQuery();
			return rs;
		} catch (SQLException e) {
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối CONTROLDB");
			return null;
		}
	}

	public void createTableWarehouse(String tableName, String list_field, int number_colum,
			String list_colum_datatype) {
		try {
			StringTokenizer stklist = new StringTokenizer(list_field, "|");
			StringTokenizer stkDatatype = new StringTokenizer(list_colum_datatype, "|");
			Connection con = DBConnection.getConnection("WAREHOUSE");
			StringBuffer sb = new StringBuffer("CREATE table ");
			sb.append(tableName);
			sb.append(" ( id INT(11) AUTO_INCREMENT PRIMARY KEY, ");
			while (stklist.hasMoreTokens() && stkDatatype.hasMoreElements()) {
				sb.append(stklist.nextToken());
				sb.append(" " + stkDatatype.nextToken());
				sb.append(", ");
			}
			sb.append("expired DATE )");
			System.out.println(sb.toString());
			PreparedStatement pre = con.prepareStatement(sb.toString());
			pre.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối STAGING");
//			return null;
		}
	}

	// lấy tất cả đữ liệu trong bảng staging
	public ResultSet getDataStagingFromDb(String tableNameInStaging) {
		try {
			Connection con = DBConnection.getConnection("STAGING");
			String sql = "Select * from " + tableNameInStaging;
			PreparedStatement pre = con.prepareStatement(sql);
			ResultSet rs = pre.executeQuery();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối STAGING");
			return null;
		}
	}

	public PreparedStatement intsertDataToWarehouse(String tableNameInWarehouse, String listfield, int number_colum) {
		try {
			StringTokenizer stk = new StringTokenizer(listfield, "|");

			Connection con = DBConnection.getConnection("WAREHOUSE");
			StringBuffer sb = new StringBuffer("Insert into ");
			sb.append(tableNameInWarehouse);
			sb.append(" (");
			while (stk.hasMoreTokens()) {
				sb.append(stk.nextToken());
				sb.append(", ");
			}
//			sb.deleteCharAt(sb.length() - 2);
			sb.append("expired");
			sb.append(" )");
			sb.append(" VALUES (");
			for (int i = 0; i < number_colum; i++) {
				sb.append("?, ");
			}
			sb.append("?)");
			return con.prepareStatement(sb.toString());

		} catch (SQLException e) {
			e.printStackTrace();
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối STAGING");
			return null;
		}
	}

	public void updateExpiredInWareHouse(String tableName, int id) {
		Connection con = DBConnection.getConnection("WAREHOUSE");
		try {
			PreparedStatement pre = con.prepareStatement("update " + tableName + " set expired=now() where id="+id);
			pre.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean checkTableExits(String tableName) {
		String sql = "SELECT COUNT(*) FROM information_schema.`TABLES` WHERE TABLE_NAME=?";
		Connection con = DBConnection.getConnection("WAREHOUSE");
		PreparedStatement pre;
		try {
			pre = con.prepareStatement(sql);
			pre.setString(1, tableName);
			ResultSet rs = pre.executeQuery();
			rs.next();
			if (rs.getInt(1) > 0)
				return true;
			return false;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

	}

	// rút dữ liệu từ table warehouse để kiểm tra trùng;
	public ResultSet getDataFromWarehouse(String tableInWarehouse, String field, String operator, String value) {
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			String sql = "Select * from " + tableInWarehouse + " where " + field + " " + operator + " " + value
					+ " AND expired = '9999-12-30'";
			PreparedStatement pre = con.prepareStatement(sql);
			ResultSet rs = pre.executeQuery();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối WAREHOUSE");
			return null;
		}
	}

	public static void main(String[] args) throws SQLException {
		GetDataFromDB get = new GetDataFromDB();
//		System.out.println(get.checkTableExits("testa"));

	}

}
