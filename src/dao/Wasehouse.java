package dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import db.DBConnection;
import model.DataHandle;
import model.SinhVien;

public class Wasehouse {

	// getAll data wasehouse
	public static ArrayList<SinhVien> getAllSinhVien() {
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// get data wasehouse
			ArrayList<SinhVien> wasehouses = new ArrayList<SinhVien>();
			String sqlWasehouse = "select * from whsinhvien";
			Statement statement;

			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sqlWasehouse);
			while (rs.next()) {
				wasehouses.add(new SinhVien(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
						rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
						rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13)));

			}
			return wasehouses;
		} catch (SQLException e) {
			e.printStackTrace();
			return new ArrayList<SinhVien>();
		}

	}

	// cap nhat du lieu dua vao id
	public static void updateData(String id) {
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// update to wasehouse
			String sql = "update whsinhvien set expired = ? where id = ?";
			PreparedStatement ps;

			ps = con.prepareStatement(sql);
			ps.setString(2, id);
			// khong gioi han thoi gian het han
			ps.setDate(1, DataHandle.getDate());
			ps.execute();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
		}
	}

	// insert data wasehouse (1 row)
	public static void insertData(SinhVien st) {
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// insert to wasehouse
			String sql = "insert into whsinhvien values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement ps;

			ps = con.prepareStatement(sql);

			ps.setString(1, DataHandle.setId());
			ps.setString(2, st.stt);
			ps.setString(3, st.mssv);
			ps.setString(4, st.lastName);
			ps.setString(5, st.firstName);
			ps.setString(6, st.dayBorn);
			ps.setString(7, st.classId);
			ps.setString(8, st.className);
			ps.setString(9, st.phoneNumber);
			ps.setString(10, st.email);
			ps.setString(11, st.address);
			ps.setString(12, st.note);
			// khong gioi han thoi gian het han
			ps.setDate(13, new Date(9999 - 1900, 11, 31));
			ps.execute();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
		}
	}

	public static void main(String[] args) {

//		SinhVien st = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		Wasehouse.insertData(st);
//		System.out.println(Wasehouse.getAllSinhVien().toString());
//		updateData("06cee6fd-2ef5-44bb-9200-defb3b39a277");
//		System.out.println(Wasehouse.getAllSinhVien().toString());
	}

}
