package staging;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.ConnectDB;

public class ProcessStaging {
	public ArrayList<String> getStaging() {
		ArrayList<String> list = new ArrayList<String>();
		String sql = "select * from staging";
		Connection con = ConnectDB.getConnection();
		Statement statement;
		try {
			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sql);
			while(rs.next()){
	               list.add(rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getString(3)+"|"+rs.getString(4)+"|"+ rs.getString(5)+"|"+rs.getString(6)+"|"+rs.getString(7)+"|"+rs.getString(8)+"|"+rs.getString(9)+"|"+rs.getString(10)+"|"+rs.getString(11));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return list;		
	}

}
