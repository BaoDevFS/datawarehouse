package warehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.poi.ss.formula.functions.Vlookup;

import db.DBConnection;
import mail.SendMail;

public class GetDataFromDB {
	SendMail send;
	Connection connectionControl,connectionStaging,connectionWarehouse;
	public GetDataFromDB() {
		send = new SendMail();
		connectionControl=DBConnection.getConnection("CONTROLDB");
		connectionStaging =DBConnection.getConnection("STAGING");
		connectionWarehouse = DBConnection.getConnection("WAREHOUSE");
	}
	//xac dinh connect vs database
	public void doSpecialTaskInLog(int idLog) {
//		Connection connectionControl = DBConnection.getConnection("CONTROLDB");
		try {
			String sql = "Select sql_for_special_task from logs where id =?";
			PreparedStatement pre = connectionControl.prepareStatement(sql);
			pre.setInt(1, idLog);
			ResultSet rs = pre.executeQuery();
			if(rs.next()) {
				sql = rs.getString(1);
				if(sql==null) {
					return;
				}
				String[] ar = sql.split("\\|");
				System.out.println(ar[0]);
				System.out.println(ar[1]);
				rs.close();
				connectionControl.close();
				connectionControl = DBConnection.getConnection(ar[0]);
				pre = connectionControl.prepareStatement(ar[1]);
				pre.executeUpdate();
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
				
	}
	public ResultSet getDataInConfig(int id) {
		try {
//			Connection connectionControl = DBConnection.getConnection("CONTROLDB");
			String sql = "Select * from config where id=?";
			PreparedStatement pre = connectionControl.prepareStatement(sql);
			pre.setInt(1, id);
			ResultSet rs = pre.executeQuery();
			return rs;
		} catch (SQLException e) {
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối CONTROLDB");
			return null;
		}
	}

	public void updateStatus(String status, int id, int rowcount) {
		try {
			String sql = "Update logs set status_file =?, time_warehouse=now(), warehouse_load_row = ? where id=?";
			Connection con = DBConnection.getConnection("CONTROLDB");
			PreparedStatement pre = con.prepareStatement(sql);
			pre.setString(1, status);
			pre.setInt(2, rowcount);
			pre.setInt(3, id);
			pre.executeUpdate();
			System.out.println("Update ok");
			pre.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
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
	public String cutField(String token) {
		if(token.contains("#")) {
			return token.substring(0, token.indexOf("#"));
		}
		return token;
	}
	//true thi tao thanh cong
	public boolean createTableWarehouse(String tableName, String list_field, int number_colum,
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
				// cắt kiểu dữ liệu trong trường hợp chứa dữ liệu để tranform khác #
				sb.append(" " + cutField(stkDatatype.nextToken()));
				sb.append(", ");
			}
			sb.append("expired DATE )");
//			System.out.println(sb.toString());
			PreparedStatement pre = con.prepareStatement(sb.toString());
			pre.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối STAGING");
			return false;
		}
	}

	public int getIdFormTableInWarehouse(String tableName, String field, String value) {
		Connection con = DBConnection.getConnection("WAREHOUSE");
		String sql;
		try {
			// warehouse datedim không có trường expired
			if(tableName.equals("datedim")) {
				sql="Select id from " + tableName + " where " + field +  " = '" + value+"'";
			}else {
				sql ="Select id from " + tableName + " where " + field +  " = '" + value
						+ "' AND expired = '9999-12-30'";
			}
			
			PreparedStatement pre = con.prepareStatement(sql);
			ResultSet rs = pre.executeQuery();
			if(rs.next()) {
				return rs.getInt(1);
			} 
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
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

	public void intsertDataToWarehouse(String tableNameInWarehouse, String listfield, int number_colum, ResultSet rsdata,String list_colum_datatype,int countRow ) {
		countRow=0;
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
			PreparedStatement pre = con.prepareStatement(sb.toString());
			String[] artoken = list_colum_datatype.split("\\|");
			// set data theo kieeur dữ liệu vào
			int i = 1;
			try {
				String token;
				int tmp;
				for (; i <= number_colum; i++) {
					token = artoken[i - 1];
					// kiểm tra kiểu dữ liệu của field
					if (token.startsWith("VARCHAR")) {
						pre.setString(i, rsdata.getString(i));
					} else if (token.startsWith("INT")) {
						if (!token.contains("#")) {
							try {
								tmp = Integer.parseInt(rsdata.getString(i));
								pre.setInt(i, tmp);
							} catch (NumberFormatException e) {
								pre.setInt(i, 0);
								continue;
							}
						} else {
							// cat cac thuc tinh dang sau
							String[] tk = token.split("#");
							if (tk[3].equals("VARCHAR")) {
								int id = getIdFormTableInWarehouse(tk[1], tk[2], rsdata.getString(i));
								pre.setInt(i, id);
							} else if (tk[3].equals("INT")) {
								int id = getIdFormTableInWarehouse(tk[1], tk[2], rsdata.getString(i));
								pre.setInt(i, id);
							} else if (tk[3].equals("DATE")) {
								Date date = MainWarehouse.tranferDate(1, rsdata.getString(i));
								try {
									if (date == null) {
										tmp =Integer.parseInt(rsdata.getString(i));
										pre.setInt(i, tmp);
										continue;
									}
								} catch (NumberFormatException e) {
									pre.setInt(i, 0);
									continue;
								}
								int id = getIdFormTableInWarehouse(tk[1], tk[2],
										date.toInstant().atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toLocalDate().toString());
								pre.setInt(i, id);
							}
						}

					} else if (token.startsWith("DATE")) {
						Date date = MainWarehouse.tranferDate(1, rsdata.getString(i));
						if (date == null) {
							pre.setDate(i, new java.sql.Date(0000, 00, 00));
							continue;
						}
						pre.setObject(i, date.toInstant().atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toLocalDate());
					}
					// cos the them cac du lieu khac
				}
				pre.setDate(i, new java.sql.Date(9999 - 1900, 11, 31));
				pre.executeUpdate();
				countRow++;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			send.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "Lỗi kết nối WAREHOUSE");
		}
	}

	public void updateExpiredInWareHouse(String tableName, int id,String expiredValue) {
		Connection con = DBConnection.getConnection("WAREHOUSE");
		try {
			PreparedStatement pre = con.prepareStatement("update " + tableName + " set expired="+expiredValue+" where id=" + id);
			pre.executeUpdate();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	// da ton tai = true
	public boolean checkTableExits(String tableName) {
		String sql = "SELECT COUNT(*) FROM information_schema.`TABLES` WHERE TABLE_NAME=?";
		Connection con = DBConnection.getConnection("WAREHOUSE");
		PreparedStatement pre;
		try {
			pre = con.prepareStatement(sql);
			pre.setString(1, tableName);
			ResultSet rs = pre.executeQuery();
			rs.next();
			if (rs.getInt(1) > 0) {
				con.close();
				return true;
			}
			con.close();
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
			String sql = "Select * from " + tableInWarehouse + " where " + field + " " + operator + " '" + value
					+ "' AND expired = '9999-12-30'";
//			System.out.println(sql);
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
//		System.out.println(get.getIdFormTableInWarehouse("datedim","id", "5"));
//		System.out.println(get.checkTableExits("testa"));

	}

}
