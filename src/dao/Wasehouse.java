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
			String sqlWasehouse = "select * from sinhvien";
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
	//get data(sinhvien) base on mssv and expired
	public static SinhVien getOneSinhVien(String mssv) {
			SinhVien sv = null;
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// get data wasehouse
			String sqlWasehouse = "select * from sinhvien where mssv= '" +mssv+ "' AND expired = '9999-12-30'";
			Statement statement;
			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sqlWasehouse);
			while (rs.next()) {
				sv = new SinhVien(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
						rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
						rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13));

			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		
		}
		return sv;

	}

	// cap nhat du lieu dua vao id
	public static void updateData(int id) {
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// update to wasehouse
			String sql = "update sinhvien set expired = ? where id = ?";
			PreparedStatement ps;

			ps = con.prepareStatement(sql);
			ps.setInt(2, id);
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
			String sql = "insert into sinhvien (sst, mssv, lastname, firstname, dayborn, classid, classname, phonenumber, email, address, note, expired) values(?,?,?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement ps;

			ps = con.prepareStatement(sql);

			ps.setString(1, st.stt);
			ps.setString(2, st.mssv);
			ps.setString(3, st.lastName);
			ps.setString(4, st.firstName);
			ps.setString(5, st.dayBorn);
			ps.setString(6, st.classId);
			ps.setString(7, st.className);
			ps.setString(8, st.phoneNumber);
			ps.setString(9, st.email);
			ps.setString(10, st.address);
			ps.setString(11, st.note);
			// khong gioi han thoi gian het han
			ps.setDate(12, new Date(9999 - 1900, 11, 31));
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
		System.out.println(Wasehouse.getOneSinhVien("17130261"));
	}

}
