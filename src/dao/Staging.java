package dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import db.DBConnection;
import model.LopHoc;
import model.MonHoc;
import model.SinhVien;

public class Staging {
	
	//get all SinhVien
	public static ArrayList<SinhVien> getAllSinhVien() {
		ArrayList<SinhVien> stagings = new ArrayList<SinhVien>();
		try {
			Connection con = DBConnection.getConnection("staging");
			String sqlWasehouse = "select * from sinhvien";
			Statement statement;

			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sqlWasehouse);
			while (rs.next()) {
				stagings.add(new SinhVien("", rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
						rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
						rs.getString(10), rs.getString(11), ""));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return stagings;
	}
	
	//get all LopHoc
	public static ArrayList<LopHoc> getAllLopHoc() {
		ArrayList<LopHoc> stagings = new ArrayList<LopHoc>();
		try {
			Connection con = DBConnection.getConnection("staging");
			String sqlWasehouse = "select * from lophoc";
			Statement statement;

			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sqlWasehouse);
			while (rs.next()) {
				stagings.add(new LopHoc("", rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), ""));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return stagings;
	}
	
	//get all MonHoc
		public static ArrayList<MonHoc> getAllMonHoc() {
			ArrayList<MonHoc> stagings = new ArrayList<MonHoc>();
			try {
				Connection con = DBConnection.getConnection("staging");
				String sqlWasehouse = "select * from monhoc";
				Statement statement;

				statement = con.createStatement();
				ResultSet rs = statement.executeQuery(sqlWasehouse);
				while (rs.next()) {
					stagings.add(new MonHoc("", rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getString(5), rs.getString(6), rs.getString(7), ""));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return stagings;
		}
		

	public static void main(String[] args) {
		System.out.println(Staging.getAllSinhVien().toString());
		System.out.println(Staging.getAllLopHoc().toString());
	}
}
