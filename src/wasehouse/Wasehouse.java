package wasehouse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import db.ConnectDB;

public class Wasehouse {
	public static ArrayList<String> getWasehouse(){
		Connection con = ConnectDB.getConnection();
		//get data wasehouse
				ArrayList<String> wasehouses = new ArrayList<String>();
				String sqlWasehouse = "select * from wasehouse";
				Statement statement;
				
				try {
					statement = con.createStatement();
					ResultSet rs = statement.executeQuery(sqlWasehouse);
					while(rs.next()){
						wasehouses.add(rs.getInt(1)+"|"+rs.getString(2)+"|"+rs.getString(3)+"|"+rs.getString(4)+"|"+ rs.getString(5)+"|"+rs.getString(6)+"|"+rs.getString(7)+"|"+rs.getString(8)+"|"+rs.getString(9)+"|"+rs.getString(10)+"|"+rs.getString(11));
					}
					statement.close();
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return wasehouses;
	}

}
