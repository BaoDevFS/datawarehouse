package part2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class TranfertoStaging {

	StringBuffer stb;
	int i = 0, j = 0;
	Row row;
	Cell cell;
	Sheet sheet;
	String tableName = "staging";

	public void loadFromSourceFile() throws ClassNotFoundException, SQLException, IOException {
		// get logs
		Connection connectDB = DBConnection.getConnection(DBConnection.jdbcURL_2);
		System.out.println("c1 ok");
		Statement st = connectDB.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM logs");
		String src, delimited;
		// get one row on table
		while (rs.next()) {
			src = rs.getString("source_folder") + rs.getString("filename") + rs.getString("filetype_download");
			System.out.println(src);
			delimited = rs.getString("delimiter");
			System.out.println(delimited);

			if (src.endsWith("xlsx")) {
				try {
					loadFromXSXL(src, tableName);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else if (src.endsWith("csv") || src.endsWith("txt")) {
				try {
					loadFromCSVOrTXT(src, delimited, tableName);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else {
				System.out.println("no method");
			}

		}

	}

	private void loadFromCSVOrTXT(String source_file, String delimited, String tableName)
			throws SQLException, ClassNotFoundException, IOException {
		Connection connect = DBConnection.getConnection(DBConnection.jdbcURL_1);
		System.out.println("Connect DB Successfully");
		// check file exits
		File f = new File(source_file);
		if (!f.exists()) {
			System.out.println("File not exist!");
			return;
		}
		BufferedReader lineReader = new BufferedReader(new FileReader(f));
		String lineText = null;
		// read line in file
		lineText = lineReader.readLine();
		// split text from delimited
		StringTokenizer st = new StringTokenizer(lineText, delimited);

		String sql = "Insert into " + tableName + " values (?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pre = connect.prepareStatement(sql);

		while ((lineText = lineReader.readLine()) != null) {
			st = new StringTokenizer(lineText, delimited);
			i = 0;
			while (st.hasMoreElements()) {
				pre.setString(++i, st.nextToken());
			}
			pre.execute();
		}
		lineReader.close();
	}

	private void loadFromXSXL(String excelFile, String tableName)
			throws ClassNotFoundException, SQLException, IOException {
		Connection connect = DBConnection.getConnection(DBConnection.jdbcURL_1);
		System.out.println("Connect DB Successfully :)");
		Workbook excel = new XSSFWorkbook(excelFile);
		sheet = excel.getSheetAt(0);

		String sql = "Insert into " + tableName + " values (?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pre = connect.prepareStatement(sql);

		// get data from file and insert to table
		while ((row = sheet.getRow(i)) != null) {
			j = 0;
			while ((cell = row.getCell(j)) != null) {
				try {
					pre.setString(j + 1, cell.getStringCellValue());
				} catch (IllegalStateException e) {
					pre.setString(j + 1, String.valueOf((int) cell.getNumericCellValue()));
				}
				j++;
			}
			pre.execute();
			i++;

		}

	}

	public static void main(String[] args) {
		try {
			new TranfertoStaging().loadFromSourceFile();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
