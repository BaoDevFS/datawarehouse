
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
	String jdbcURL_1 = "jdbc:mysql://localhost/datawarehouse?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	String userName_1 = "root";
	String password_1 = "";


	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		TranferData ex = new TranferData();
//		ex.loadVPro("D:\\00_HK2_3\\DataWarehouse\\Datasource_23052020.xlsx");
		ex.copyVpro();
	}

	public void loadVPro(String excelFile) throws ClassNotFoundException, SQLException, IOException {
		Connection connect = DBConnection.getConnection(jdbcURL_1, userName_1, password_1);
		System.out.println("Connect DB Successfully :)");
		Workbook excel = new XSSFWorkbook(excelFile);
		Sheet sheet = excel.getSheetAt(0);
		Row row;
		Cell cell;
		int i = 1, j = 0;
		String sql = "CREATE TABLE data(mssv VARCHAR(100),class VARCHAR(300),department VARCHAR(255),faculty VARCHAR(255),gender VARCHAR(50), fullname VARCHAR(255),palceofbirth VARCHAR(255),schoolyear VARCHAR(255))";
		PreparedStatement preparedStatement = connect.prepareStatement(sql);
		preparedStatement.execute();
		String query = "INSERT INTO data VALUES(?, ?, ?, ?, ?,?,?,?)";
		PreparedStatement pre = connect.prepareStatement(query);
		String v1 = "", v2 = "", v3 = "", v4 = "", v5 = "", v6 = "", v7 = "", v8 = "";
		while ((row = sheet.getRow(i)) != null) {
			while ((cell = row.getCell(j)) != null) {
				if (i != 0 && j == 0) {
					v1 = String.valueOf((int) cell.getNumericCellValue());
					j++;
					continue;
				}
				switch (j) {
				case 1:
					v2 = (String)cell.getStringCellValue();
					break;
				case 2:
					v3 = (String)cell.getStringCellValue();
					break;
				case 3:
					v4 = (String)cell.getStringCellValue();
					break;
				case 4:
					v5 =(String) cell.getStringCellValue();
					break;
				case 5:
					v6 = (String)cell.getStringCellValue();
					break;
				case 6:
					v7 = (String)cell.getStringCellValue();
					break;
				case 7:
					v8 = (String)cell.getStringCellValue();
					break;
				default:
					break;
				}
				j++;
			}
			pre.setString(1, v1);
			pre.setString(2, v2);
			pre.setString(3, v3);
			pre.setString(4, v4);
			pre.setString(5, v5);
			pre.setString(6, v6);
			pre.setString(7, v7);
			pre.setString(8, v8);
			pre.execute();
			i++;
			j = 0;
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


	public void copy(String database1, String database2) throws ClassNotFoundException, SQLException {
		
//		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_2, userName_2, password_2);
//		System.out.println("c2 ok");
//
//		ResultSet rs;
//		Statement stmt = connectionDB1.createStatement();
//		rs = stmt.executeQuery("SELECT * FROM information");
//		ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();
//		int counter = md.getColumnCount();
//		String colName[] = new String[counter];
//		System.out.println("The column names are as follows:");
//		for (int loop = 1; loop <= counter; loop++) {
//			colName[loop - 1] = md.getColumnLabel(loop);
////			sqlCreateTable += colName[loop - 1] + " CHAR(50),";
//		}

		
//		sqlCreateTable += ")";

//		System.out.println(sqlCreateTable);
//		PreparedStatement p = connectionDB2.prepareStatement(sqlCreateTable);
//		p.execute();

//		COPY 
//		String insert = "INSERT INTO datacopy.information SELECT * FROM datawarehouse.information";
//		System.out.println(insert);
//		PreparedStatement pc = connectionDB2.prepareStatement(insert);
//		pc.execute();
	}
}
