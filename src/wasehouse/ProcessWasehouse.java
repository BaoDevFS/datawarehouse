package wasehouse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;


import db.ConnectDB;
import staging.ProcessStaging;
import staging.Staging;

public class ProcessWasehouse {
	
	
	// y tuong
	// load staging 1 row, kiem tra co trung lap trong wasehouse khong, neu trung lap thi....
	public void testDuplicateData() {
		ArrayList<String> wasehouses = Wasehouse.getWasehouse();
		ArrayList<String> stagings = Staging.getStaging();
		System.out.println(wasehouses.toString());
		System.out.println();
		System.out.println(stagings.toString());
			}
	
	//test ne
	public static void test() {
		ArrayList<String> list = Wasehouse.getWasehouse();
		System.out.println(list.toString());
		String sqlStaging = "select * from staging";
		Connection con = ConnectDB.getConnection();
		Statement statement;
		ResultSet rs;
		try {
			statement = con.createStatement();
			rs = statement.executeQuery(sqlStaging);
			String result = "";
			while(rs.next()){
					result = rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getString(3)+"|"+rs.getString(4)+"|"+ rs.getString(5)+"|"+rs.getString(6)+"|"+rs.getString(7)+"|"+rs.getString(8)+"|"+rs.getString(9)+"|"+rs.getString(10)+"|"+rs.getString(11);
					for(int i = 0; i<list.size(); i++) {
						if(result.equals(list.get(i))) {
							// du lieu giong nhau, cap nhat lai expired trong wasehouse
						}else {
							// du lieu khac nhau, insert du lieu vao wasehouse
						}
					}
			}
			statement.close();
			rs.close();
//			rsWasehouse.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
		
	//------------------------------
	//chuyen du lieu tu staging sang wasehouse
	public void transport() {
		String sql = "insert into wasehouse values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?)";

		ProcessStaging processStaging = new ProcessStaging();
		ArrayList<String> list = processStaging.getStaging();
		
		Connection con = ConnectDB.getConnection();
		
		PreparedStatement preparedStatement;
		
		StringTokenizer st;
		
		try {
			preparedStatement = con.prepareStatement(sql);

			for(int i = 0; i<list.size(); i++) {
				st = new StringTokenizer(list.get(i),"|");
				int count = 0;
				int size = st.countTokens();
				while(st.hasMoreTokens()) {
					count++;
					if(st.countTokens() == 1) {
						preparedStatement.setDate(12, new Date(-1900,0,2));
					}
					if(size - st.countTokens() == 0) {
						preparedStatement.setInt(count, Integer.parseInt(st.nextToken()));
					}else {
						preparedStatement.setString(count, st.nextToken());
					}
				}
				preparedStatement.execute();
			}
			
			preparedStatement.close();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}
}
