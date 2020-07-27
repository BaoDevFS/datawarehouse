package part2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.Handle;
import db.DBConnection;

public class TranfertoStaging {
	StringBuffer stb;
	Row row;
	Cell cell;
	Sheet sheet;
	String tableName = "staging";
	private Timer timer;

	public void startTask(int idRowConfig) {
		timer = new Timer();
		TimerTask timerTask = new TimerTask() {
			@Override
			public void run() {
				try {
					loadFromSourceFile(idRowConfig);
				} catch (ClassNotFoundException | SQLException | IOException e) {
					e.printStackTrace();
				}
			}
		};
		timer.schedule(timerTask, 0, 1 * 60 * 1000);
	}

	// jdbcURL_2 là controldb, jdbcURL_1 là staging database
	public void loadFromSourceFile(int idConfig) throws ClassNotFoundException, SQLException, IOException {
		// get logs
		String dir = "", src, delimited, status, file_format_start_with = "",tableName="";
		int idFile = 0;
		// Mở kết nối với controldb
		Connection connectDB = DBConnection.getConnection("CONTROLDB");
		// st1 để lấy folder đang lưu các file dữ liệu, st để lấy tên của từng file
		Statement st1 = connectDB.createStatement();
		ResultSet rs1 = st1.executeQuery("SELECT * FROM config where id=" + idConfig);
		if (rs1.next()) {
			dir = rs1.getString("download_to_dir_local");
			file_format_start_with = rs1.getString("file_format_start_with");
			tableName= rs1.getString("table_name_staging");
		} else {
			System.out.println("Không có bản ghi nào có config id là " + idConfig);
			return;
		}
		System.out.println("Directory: " + dir);
		Statement st = connectDB.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM logs where id_config=" + idConfig);

		// get one row on table
		while (rs.next()) {
			src = dir + rs.getString("filename");
			System.out.println(src);
			delimited = rs.getString("delimiter");
			System.out.println(delimited);
			status = rs.getString("status_file");
			System.out.println("status là " + status);
			idFile = rs.getInt("id");

			// nếu status_file là ER thì mới chuyển qua staging
			if (!status.equalsIgnoreCase("ER"))
				continue;
			// nếu file không tồn tại thì chuyển status_file là FILE_NOT_FOUND
			File f = new File(src);
			if (!f.exists()) {
				System.out.println("File not exist!");
				// update status file
				updateStatus("FILE_NOT_FOUND", idFile);
				continue;
			}
			System.out.println("FileName: " + src
					+ " -------------------------------------------------------------------------------------------------------------------------");
			// dựa vào đuôi của file mà chuyển vô staging khác nhau
			if (src.endsWith("xlsx")) {
				try {
					loadFromXSXL(src, file_format_start_with,tableName, idFile);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else if (src.endsWith("csv") || src.endsWith("txt")) {
				try {
					loadFromCSVOrTXT(src, delimited, file_format_start_with,tableName, idFile);
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} else {
				System.out.println("no method");
			}

//			Handle.convertDataFromStagingToWasehouse(String.valueOf(idFile));

			// sau khi đưa dữ liệu vào warehouse thì truncate bảng staging
//			truncateTable("STAGING", file_format_start_with);
		}

	}

	// phương thức dùng để update status của một dòng trong bảng logs bằng một
	// status nào đó
	public void updateStatus(String status, int id) throws ClassNotFoundException, SQLException {
		String sql = "Update logs set status_file =? where id=?";
		Connection con = DBConnection.getConnection("CONTROLDB");
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setString(1, status);
		pre.setInt(2, id);
		pre.executeUpdate();
		System.out.println("Update ok");
		pre.close();

	}

	// phương thức dùng để update các trường của một dòng sau khi đưa vô staging
	// thành công
	public void updateFile(int id, int size, int total_row, int staging_load_row)
			throws SQLException, ClassNotFoundException {
		String sql = "Update logs set size=?,total_row=?,staging_load_row=?,time_staging=?,status_file=? where id=?";
		Connection con = DBConnection.getConnection("CONTROLDB");
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setInt(1, size);
		pre.setInt(2, total_row);
		pre.setInt(3, staging_load_row);
		pre.setDate(4, new Date(System.currentTimeMillis()));
		pre.setString(5, "OK STAGING");
		pre.setInt(6, id);
		pre.executeUpdate();
		System.out.println("Update ok");
		pre.close();

	}

	public void truncateTable(String database, String tableName) throws SQLException {
		String sql = "TRUNCATE TABLE " + tableName;
		Connection con = DBConnection.getConnection(database);
		PreparedStatement pre = con.prepareStatement(sql);
		pre.executeUpdate();
		pre.close();

		System.out.println("Truncate ok");

	}

	public String loadFile(File f, String tableName, String delimited, int numberOfField)
			throws SQLException, IOException {
		Connection connect = DBConnection.getConnection("STAGING");
		BufferedReader lineReader = new BufferedReader(new FileReader(f));
		String lineText = "";
		lineText = lineReader.readLine();
		int total_row = 0, staging_load_row = 0, numberField = numberOfField;
		String sql = "Insert into " + tableName + " values (";
		while (numberField > 0) {
			if (numberField != 1) {
				sql += "?,";
			} else
				sql += "?";
			numberField--;
		}
		sql += ")";
		PreparedStatement pre = connect.prepareStatement(sql);

		// xử lý các dòng còn lại trong file
		while ((lineText = lineReader.readLine()) != null) {
			total_row++;
			System.out.println("lineText: " + lineText);
			// tách dòng theo delimit
			StringTokenizer st = new StringTokenizer(lineText, delimited);
			System.out.println("Count token: " + st.countTokens());
			int i = 0;
			// dòng không đủ field thì không đưa vô staging, đọc dòng tiếp theo, đủ field
			// thì đưa vô staging
			if (st.countTokens() == numberOfField) {
				while (st.hasMoreTokens()) {
					String tken = st.nextToken();
					System.out.println(tken);
					pre.setString(++i, (tken));
				}
				pre.execute();
				staging_load_row++;
			}
		}
		lineReader.close();
		return total_row + "-" + staging_load_row;
	}

// phương thức dùng để tải nội dung trong file csv hoặc txt vào bảng staging
	private void loadFromCSVOrTXT(String source_file, String delimited,String file_format_start_with, String tableName, int id)
			throws SQLException, ClassNotFoundException, IOException {
		System.out.println("TẢI FILE TXT");
		// staging_load_row là số dòng đưa vô staging, total_row là tổng số dòng của
		// file
		int staging_load_row = 0, total_row = 0;
		System.out.println("Connect DB Successfully");
		File f = new File(source_file);
		StringTokenizer token = null;

		if (file_format_start_with.equalsIgnoreCase("sinhvien")) {
			token = new StringTokenizer(loadFile(f, tableName, delimited, 11), "-");

		} else if (file_format_start_with.equalsIgnoreCase("lophoc")) {
			token = new StringTokenizer(loadFile(f, tableName, delimited, 4), "-");

		} else if (file_format_start_with.equalsIgnoreCase("dangky")) {
			token = new StringTokenizer(loadFile(f, tableName, delimited, 5), "-");

		} else if (file_format_start_with.equalsIgnoreCase("Monhoc")) {
			token = new StringTokenizer(loadFile(f, tableName, delimited, 7), "-");

		}
		total_row = Integer.parseInt(token.nextToken());
		staging_load_row = Integer.parseInt(token.nextToken());
		// đọc xong 1 file, update lại trong logs table
		updateFile(id, (int) f.length(), total_row, staging_load_row);

	}

	public int loadFileXSXL(File f, String tableName, int numberOfField)
			throws SQLException, InvalidFormatException, IOException {
		int numberField = numberOfField;
		Connection connect = DBConnection.getConnection("STAGING");
		System.out.println("Connect DB Successfully");
		Workbook excel = new XSSFWorkbook(f);
		sheet = excel.getSheetAt(0);

		String sql = "Insert into " + tableName + " values (";
		while (numberField > 0) {
			if (numberField != 1) {
				sql += "?,";
			} else
				sql += "?";
			numberField--;
		}
		sql += ")";

		PreparedStatement pre = connect.prepareStatement(sql);

		// get data from file and insert to table
//bỏ hàng 0 đọc từ hàng 1
		int i = 1;
		while ((row = sheet.getRow(i)) != null) {
			// đọc numberOfField cột trên dòng
			for (int j = 0; j < numberOfField; j++) {
				cell = row.getCell(j);

				try {
					System.out.println(cell.getStringCellValue());
					pre.setString(j + 1, cell.getStringCellValue());

				} catch (IllegalStateException e) {
					pre.setString(j + 1, String.valueOf((int) cell.getNumericCellValue()));

				} catch (NullPointerException e) {
					// nếu bị null pointer thì điền vô staging là rỗng
					pre.setString(j + 1, "");
				}
			}
			pre.execute();
			i++;

		}

		return i - 1;
	}

	private void loadFromXSXL(String excelFile,String file_format_start_with, String tableName, int id)
			throws ClassNotFoundException, SQLException, IOException, InvalidFormatException {
		int total_row = 0;
		System.out.println("TẢI FILE XSXL");
// Mở file
		File f = new File(excelFile);
		if (file_format_start_with.equalsIgnoreCase("sinhvien")) {
			total_row = loadFileXSXL(f,tableName, 11);
		} else if (file_format_start_with.equalsIgnoreCase("lophoc")) {
			total_row = loadFileXSXL(f, tableName, 4);
		} else if (file_format_start_with.equalsIgnoreCase("dangky")) {
			total_row = loadFileXSXL(f, tableName, 5);
		} else if (file_format_start_with.equalsIgnoreCase("Monhoc")) {
			total_row = loadFileXSXL(f, tableName, 7);
		}

		// đọc xong 1 file, cập nhật lại các field của trong logs
		updateFile(id, (int) f.length(), total_row, total_row);

	}

	// import data from csv or txt using load data infile
	private void loadFromCSVOrTXT1(String source_file, String delimited, String tableName)
			throws ClassNotFoundException, SQLException {
		Connection connect = DBConnection.getConnection("STAGING");
		System.out.println("Connect DB Successfully");
		String sql = "LOAD DATA INFILE " + "'" + source_file + "'" + " INTO TABLE " + tableName
				+ " FIELDS TERMINATED BY '" + delimited + "'" + " IGNORE 1 LINES;";

		PreparedStatement pre = connect.prepareStatement(sql);
		pre.executeQuery();
		System.out.println("Load success");
	}

	public static void main(String[] args) throws SQLException {
		try {

			new TranfertoStaging().loadFromSourceFile(4);

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
