package wasehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;


import db.ConnectDB;
import staging.ProcessStaging;

public class ProcessWasehouse {
	
	public void getData(String url,String user,String password, String urlStaging) {
		String sql = "INSERT INTO wasehouse VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		ProcessStaging processStaging = new ProcessStaging();
		ArrayList<String> list = processStaging.getData(urlStaging, user, password);
		
		Connection con = ConnectDB.getConnection(url, user, password);
		
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
					if(size - st.countTokens() == 0 || size - st.countTokens() == 1) {
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
