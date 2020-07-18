package staging;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import db.ConnectDB;

public class Staging {
	
	public static ArrayList<String> getStaging(){
		Connection con = ConnectDB.getConnection();
		//get list staging
		ArrayList<String> stagings = new ArrayList<String>();
		String sqlStaging = "select * from staging";
		Statement statement;
		try {
			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sqlStaging);
			while(rs.next()){
				stagings.add(rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getString(3)+"|"+rs.getString(4)+"|"+ rs.getString(5)+"|"+rs.getString(6)+"|"+rs.getString(7)+"|"+rs.getString(8)+"|"+rs.getString(9)+"|"+rs.getString(10)+"|"+rs.getString(11));
			}
//			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return stagings;
	}

}
