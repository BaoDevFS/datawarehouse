
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class TranferData {
	String jdbcURL_1 = "jdbc:mysql://localhost/controldb?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	String userName_1 = "root";
	String password_1 = "";

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		TranferData ex = new TranferData();
//		ex.load("D:\\00_HK2_3\\DataWarehouse\\Datasource_23052020.xlsx");
//		ex.copyVpro();
		ex.loadV2();
	}

	public void loadV2() throws ClassNotFoundException, SQLException, IOException {
		Connection connectDB = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		System.out.println("c1 ok");
		Statement st = connectDB.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM config");
		rs.next();
		String src = rs.getString("source");
		System.out.println(src);
		String des = rs.getString("destination");
		System.out.println(des);
		String user_des = rs.getString("user_des");
		System.out.println(user_des);
		String pw_des = rs.getString("pw_des");
		System.out.println(pw_des);
		String tableName = src.substring(src.lastIndexOf("\\") + 1, src.lastIndexOf("."));
		System.out.println(tableName);
		if (src.endsWith("xlsx")) {
			loadFromXSXL(src, tableName, des, user_des, pw_des);
		} else {
			System.out.println("no method");
		}

	}

	public void loadFromXSXL(String excelFile, String tableName, String sourcDb, String user_sr, String password)
			throws ClassNotFoundException, SQLException, IOException {
		Connection connect = DBConnection.getConnection(sourcDb, user_sr, password);
		System.out.println("Connect DB Successfully :)");
		Workbook excel = new XSSFWorkbook(excelFile);
		Sheet sheet = excel.getSheetAt(0);
		Row row;
		Cell cell;
		int i = 1, j = 0;
//		String sql = "CREATE TABLE "
//				+ "data(mssv VARCHAR(100),class VARCHAR(300),"
//				+ "department VARCHAR(255),faculty VARCHAR(255),"
//				+ "gender VARCHAR(50), fullname VARCHAR(255),"
//				+ "palceofbirth VARCHAR(255),schoolyear VARCHAR(255))";
		StringBuffer stb = new StringBuffer("CREATE TABLE ");
		stb.append(tableName);
		stb.append("(");
		row = sheet.getRow(0);
		while ((cell = row.getCell(j)) != null) {
			stb.append(cell.getStringCellValue());
			stb.append(" VARCHAR(255),");
			j++;
		}
		stb.deleteCharAt(stb.length() - 1);
		stb.append(")");
		System.out.println(stb.toString());
		PreparedStatement preparedStatement = connect.prepareStatement(stb.toString());
		preparedStatement.execute();
		stb = new StringBuffer("INSERT INTO ");
		stb.append(tableName);
		stb.append(" VALUES");
		stb.append("(");
		for (int k = 0; k < j; k++) {
			stb.append("?, ");
		}
		stb.deleteCharAt(stb.length() - 2);
		stb.append(")");
//		String query = "INSERT INTO data VALUES(?, ?, ?, ?, ?,?,?,?)";
		System.out.println(stb.toString());
		PreparedStatement pre = connect.prepareStatement(stb.toString());
		
		while ((row = sheet.getRow(i)) != null) {
			j = 0;
			while ((cell = row.getCell(j)) != null) {
				try {
					pre.setString(j + 1, cell.getStringCellValue());
					System.out.println(cell.getStringCellValue());
				} catch (IllegalStateException e) {
					pre.setString(j + 1, String.valueOf((int) cell.getNumericCellValue()));
				}
				j++;
			}
			pre.execute();
			i++;
			
		}

	}

	public void copyVpro() throws ClassNotFoundException, SQLException {
		Connection connectDB = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		System.out.println("c1 ok");
		String sql = "CREATE TABLE dataresult(mssv INTEGER(8),class VARCHAR(300),department VARCHAR(255),faculty VARCHAR(255),gender VARCHAR(50), fullname VARCHAR(255),palceofbirth VARCHAR(255),schoolyear VARCHAR(255))";
		PreparedStatement pc = connectDB.prepareStatement(sql);
		pc.execute();
		Statement st = connectDB.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM data");
		String query = "INSERT INTO dataresult VALUES(?, ?, ?, ?, ?,?,?,?)";
		PreparedStatement pre = connectDB.prepareStatement(query);
		while (rs.next()) {
			pre.setInt(1, Integer.parseInt(rs.getString(1)));
			pre.setString(2, rs.getString(2));
			pre.setString(3, rs.getString(3));
			pre.setString(4, rs.getString(4));
			pre.setString(5, rs.getString(5));
			pre.setString(6, rs.getString(6));
			pre.setString(7, rs.getString(7));
			pre.setString(8, rs.getString(8));
			pre.execute();
		}
	}

}
