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
import model.LopHoc;
import model.MonHoc;
import model.SinhVien;

public class Wasehouse {

	// getAll data sinhvien
	public static ArrayList<SinhVien> getAllSinhVien() {
		ArrayList<SinhVien> wasehouses = new ArrayList<SinhVien>();
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// get data wasehouse
			String sqlWasehouse = "select * from sinhvien";
			Statement statement;

			statement = con.createStatement();
			ResultSet rs = statement.executeQuery(sqlWasehouse);
			while (rs.next()) {
				wasehouses.add(new SinhVien(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
						rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9),
						rs.getString(10), rs.getString(11), rs.getString(12), rs.getString(13)));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return wasehouses;

	}
	
	// getAll data LopHoc
		public static ArrayList<LopHoc> getAllLopHoc() {
			ArrayList<LopHoc> wasehouses = new ArrayList<LopHoc>();
			try {
				Connection con = DBConnection.getConnection("WAREHOUSE");
				// get data wasehouse
				
				String sqlWasehouse = "select * from lophoc";
				Statement statement;

				statement = con.createStatement();
				ResultSet rs = statement.executeQuery(sqlWasehouse);
				while (rs.next()) {
					wasehouses.add(new LopHoc(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getString(5), rs.getString(6)));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return wasehouses;

		}
		
		// getAll data monhoc
		public static ArrayList<MonHoc> getAllMonHocs() {
			ArrayList<MonHoc> wasehouses = new ArrayList<MonHoc>();
			try {
				Connection con = DBConnection.getConnection("WAREHOUSE");
				// get data wasehouse
				String sqlWasehouse = "select * from monhoc";
				Statement statement;

				statement = con.createStatement();
				ResultSet rs = statement.executeQuery(sqlWasehouse);
				while (rs.next()) {
					wasehouses.add(new MonHoc(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9)));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return wasehouses;

		}
		
		//----------------------------------------------------------------------------------
	
	
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
	
	//get data(lophoc) base on mssv and expired
		public static LopHoc getOneLopHoc(String maLop) {
				LopHoc sv = null;
			try {
				Connection con = DBConnection.getConnection("WAREHOUSE");
				// get data wasehouse
				String sqlWasehouse = "select * from lophoc where malop= '" +maLop+ "' AND expired = '9999-12-30'";
				Statement statement;
				statement = con.createStatement();
				ResultSet rs = statement.executeQuery(sqlWasehouse);
				while (rs.next()) {
					sv = new LopHoc(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getString(5), rs.getString(6));

				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			
			}
			return sv;

		}
		
		//get data(monhoc) base on mssv and expired
		public static MonHoc getOneMonHoc(String mssv) {
				MonHoc sv = null;
			try {
				Connection con = DBConnection.getConnection("WAREHOUSE");
				// get data wasehouse
				String sqlWasehouse = "select * from monhoc where ma_mh= '" +mssv+ "' AND expired = '9999-12-30'";
				Statement statement;
				statement = con.createStatement();
				ResultSet rs = statement.executeQuery(sqlWasehouse);
				while (rs.next()) {
					sv = new MonHoc(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getString(9));
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			
			}
			return sv;

		}

		
		//----------------------------------------------------------------------------
	// cap nhat du lieu dua vao id
	public static void updateData(int id, String nameTable) {
		try {
			Connection con = DBConnection.getConnection("WAREHOUSE");
			// update to wasehouse
			String sql = "update "+ nameTable+" set expired = ? where id = ?";
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

	
	//-----------------------------------------
	
	
	// insert data wasehouse (1 row) sinhvien
	public static void insertDataSinhVien(SinhVien st) {
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
	
	// insert data wasehouse (1 row) lophoc
		public static void insertDataLopHoc(LopHoc lh) {
			try {
				Connection con = DBConnection.getConnection("WAREHOUSE");
				String sql = "insert into lophoc (stt, malop, monhoc, namhoc, expired) values(?,?,?,?,?)";
				PreparedStatement ps;

				ps = con.prepareStatement(sql);

				ps.setString(1, lh.stt);
				ps.setString(2, lh.maLop);
				ps.setString(3, lh.maMon);
				ps.setString(4, lh.namHoc);
				// khong gioi han thoi gian het han
				ps.setDate(5, new Date(9999 - 1900, 11, 31));
				ps.execute();

			} catch (SQLException e) {

				System.out.println(e.getMessage());
			}
		}
		
		// insert data wasehouse (1 row) monhoc
		public static void insertDataMonHoc(MonHoc mh) {
			try {
				Connection con = DBConnection.getConnection("WAREHOUSE");
				// insert to wasehouse
				String sql = "insert into monhoc (stt, ma_mh, ten_mh, tinchi, quanly, sudung, ghichu, expired) values(?,?,?,?,?,?,?,?)";
				PreparedStatement ps;

				ps = con.prepareStatement(sql);

				ps.setString(1, mh.stt);
				ps.setString(2, mh.maMH);
				ps.setString(3, mh.tenMH);
				ps.setString(4, mh.tinChi);
				ps.setString(5, mh.quanLy);
				ps.setString(6, mh.suDung);
				ps.setString(7, mh.ghiChu);
				// khong gioi han thoi gian het han
				ps.setDate(8, new Date(9999 - 1900, 11, 31));
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
//		System.out.println(Wasehouse.getOneSinhVien("17130261"));
	}

}
