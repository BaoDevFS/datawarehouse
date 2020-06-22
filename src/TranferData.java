
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

public class TranferData {
//	String jdbcURL_1 = "jdbc:mysql://localhost/controldb?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
//	String userName_1 = "root";
//	String password_1 = "";
	String jdbcURL_1 = "jdbc:mysql://localhost/datawarehouse?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
	String userName_1 = "root";
	String password_1 = "";

	StringBuffer stb;
	int i = 0, j = 0;
	Row row;
	Cell cell;
	Sheet sheet;

	public static void main(String[] args) throws Exception {
		TranferData ex = new TranferData();
		ex.copyVpro();
//		ex.loadFromSourceFile();
//		convertSelectedSheetInXLXSFileToCSV(new File("D:\\00_HK2_3\\DataWarehouse\\17130010_Datasource_23052020.xlsx"), 0);
	}

	public void loadFromSourceFile() throws ClassNotFoundException, SQLException, IOException {
		// get config
		Connection connectDB = DBConnection.getConnection();
		System.out.println("c1 ok");
		Statement st = connectDB.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM config");
		String src,des,user_des,pw_des,tableName,delimited;
		// get one row on table
		while (rs.next()) {
			src = rs.getString("source");
			System.out.println(src);
			des = rs.getString("destination");
			System.out.println(des);
			user_des = rs.getString("user_des");
			System.out.println(user_des);
			pw_des = rs.getString("pw_des");
			System.out.println(pw_des);
			tableName = src.substring(src.lastIndexOf("\\") + 1, src.lastIndexOf("."));
			System.out.println(tableName);
			delimited = rs.getString("delimited");
			System.out.println(delimited);

			if (src.endsWith("xlsx")) {
				try {
					loadFromXSXL(src, tableName, des, user_des, pw_des);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else if (src.endsWith("csv")||src.endsWith("txt")) {
				try {
					loadFromCSVOrTXT(src, delimited, tableName, des, user_des, pw_des);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else {
				System.out.println("no method");
			}

		}

	}

	private void loadFromCSVOrTXT(String source_file, String delimited, String tableName, String des_db, String user_sr,
			String password) throws SQLException, ClassNotFoundException, IOException {
		Connection connect = DBConnection.getConnection();
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
		// create query new table
		StringBuffer stb = new StringBuffer();
		stb.append("CREATE table ");
		stb.append(tableName);
		stb.append("(");
		i = 0;
		while (st.hasMoreElements()) {
			stb.append(st.nextToken().replaceAll("[)., ]", "_"));
			stb.append(" CHAR(255),");
			i++;
		}
		stb.deleteCharAt(stb.length() - 1);
		stb.append(")");
		System.out.println(stb.toString());
		// excute query
		PreparedStatement preparedStatement = connect.prepareStatement(stb.toString());
		preparedStatement.execute();
		
		System.out.println("Create table Successfully");
		// create query insert data to table
		stb = new StringBuffer();
		stb.append("INSERT INTO ");
		stb.append(tableName);
		stb.append(" VALUES(");
		System.out.println(i);
		j = 0;
		for (; j < i; j++) {
			stb.append("?, ");
		}
		stb.deleteCharAt(stb.length() - 2);
		stb.append(")");
		System.out.println(stb.toString());
		PreparedStatement pre = connect.prepareStatement(stb.toString());
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

	private void loadFromXSXL(String excelFile, String tableName, String des_db, String user_sr, String password)
			throws ClassNotFoundException, SQLException, IOException {
		Connection connect = DBConnection.getConnection();
		System.out.println("Connect DB Successfully :)");
		Workbook excel = new XSSFWorkbook(excelFile);
		sheet = excel.getSheetAt(0);
		i = 1;
		j = 0;
		// Create new table
		StringBuffer stb = new StringBuffer("CREATE TABLE ");
		stb.append(tableName);
		stb.append("(");
		row = sheet.getRow(0);
		while ((cell = row.getCell(j)) != null) {
			stb.append(cell.getStringCellValue().replaceAll("[)(., ]", "_"));
			stb.append(" VARCHAR(255),");
			j++;
		}
		stb.deleteCharAt(stb.length() - 1);
		stb.append(")");
		System.out.println(stb.toString());

		// excute query
		PreparedStatement preparedStatement = connect.prepareStatement(stb.toString());
		preparedStatement.execute();

		// insert data to table
		stb = new StringBuffer("INSERT INTO ");
		stb.append(tableName);
		stb.append(" VALUES");
		stb.append("(");
		for (int k = 0; k < j; k++) {
			stb.append("?, ");
		}
		stb.deleteCharAt(stb.length() - 2);
		stb.append(")");

		System.out.println(stb.toString());
		PreparedStatement pre = connect.prepareStatement(stb.toString());

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
	public  void convertSelectedSheetInXLXSFileToCSV(File xlsxFile, int sheetIdx) throws Exception {
		 
        FileInputStream fileInStream = new FileInputStream(xlsxFile);
 
        // Open the xlsx and get the requested sheet from the workbook
        XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
        XSSFSheet selSheet = workBook.getSheetAt(sheetIdx);
 
        // Iterate through all the rows in the selected sheet
        Iterator<Row> rowIterator = selSheet.iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
 
            // Iterate through all the columns in the row and build ","
            // separated string
            Iterator<Cell> cellIterator = row.cellIterator();
            StringBuffer sb = new StringBuffer();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                if (sb.length() != 0) {
                    sb.append(",");
                }
                // If you are using poi 4.0 or over, change it to
                // cell.getCellType
                switch (cell.getCellType()) {
                case STRING:
                    sb.append(cell.getStringCellValue());
                    break;
                case NUMERIC:
                    sb.append((int)cell.getNumericCellValue());
                    break;
                case BOOLEAN:
                    sb.append(cell.getBooleanCellValue());
                    break;
                default:
                }
            }
            System.out.println(sb.toString());
        }
        workBook.close();
    }
	public void copyVpro() throws ClassNotFoundException, SQLException {
		Connection connectDB = DBConnection.getConnection();
		System.out.println("c1 ok");
		String sql = "LOAD DATA INFILE 'D:/xampp/mysql/data/datawarehouse/data/Data_17130256.csv' INTO TABLE table2 CHARACTER SET utf8 FIELDS TERMINATED BY ',' IGNORE 1 ROWS(Emp_ID,First_Name,Last_Name,E_Mail,Date_Of_Birth,Salary,Phone_No,City)";
		PreparedStatement pc = connectDB.prepareStatement(sql);
		pc.execute();

//		Statement st = connectDB.createStatement();
//		ResultSet rs = st.executeQuery("SELECT * FROM data");
//		String query = "INSERT INTO dataresult VALUES(?, ?, ?, ?, ?,?,?,?)";
//		PreparedStatement pre = connectDB.prepareStatement(query);
//
//		while (rs.next()) {
//			pre.setInt(1, Integer.parseInt(rs.getString(1)));
//			pre.setString(2, rs.getString(2));
//			pre.setString(3, rs.getString(3));
//			pre.setString(4, rs.getString(4));
//			pre.setString(5, rs.getString(5));
//			pre.setString(6, rs.getString(6));
//			pre.setString(7, rs.getString(7));
//			pre.setString(8, rs.getString(8));
//			pre.execute();
//		}
	}

}
