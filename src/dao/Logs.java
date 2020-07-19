package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import db.DBConnection;
import model.DataHandle;

public class Logs {
	
	//cap nhat du lieu dua vao id
		public static void updateDataLogs(String id, String status) {
			try {
			Connection con = DBConnection.getConnection("CONTROLDB");
			//update to wasehouse
			String sql = "update logs set status_file = ?, time_datawarehouse = ? where id = ?";
			PreparedStatement ps;
			
				ps = con.prepareStatement(sql);
				ps.setString(1, status);
				//khong gioi han thoi gian het han
				ps.setDate(2, DataHandle.getDate());
				ps.setString(3, id);
				ps.execute();
				
			} catch (SQLException e) {
				
				System.out.println(e.getMessage());
			}
		}
		public static void main(String[] args) {
			updateDataLogs("1", "OK");
		}

}
