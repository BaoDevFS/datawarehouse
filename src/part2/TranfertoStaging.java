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
	
	public void loadFromSourceFile(int idConfig) throws ClassNotFoundException, SQLException, IOException {
		String filePath, delimited, table_name_staging = "",list_field_name="",sql = "";
		int idFile = 0;
		// Mở kết nối với controldb
		Connection connectDB = DBConnection.getConnection("CONTROLDB");
		// chọn ra tất cả các bản ghi có id_config truyền vào và status là ER
		sql = "SELECT * FROM config c JOIN logs l on c.id = l.id_config where c.id=? AND l.status_file=?";
		PreparedStatement pre = connectDB.prepareStatement(sql);
		pre.setInt(1, idConfig);
		pre.setString(2, "ER");
		ResultSet rs = pre.executeQuery();
		if (rs.next()) {
			filePath = rs.getString("download_to_dir_local") + rs.getString("filename");
			table_name_staging = rs.getString("table_name_staging");
			list_field_name = rs.getString("list_field_name");
			System.out.println("Source file: " + filePath);
			delimited = rs.getString("delimiter");
			System.out.println(delimited);
			idFile = rs.getInt("l.id");

			File file = new File(filePath);
			if (!file.exists()) {
				System.out.println("File not exist!");
				// update status file
				updateStatus("FILE_NOT_FOUND", idFile);

			} else {

				// dựa vào đuôi của file mà chuyển vô staging khác nhau
				if (filePath.endsWith("xlsx")) {
					try {
						 loadFileXSXL(file,  table_name_staging,  list_field_name, idFile);
					} catch (Exception e) {
						e.printStackTrace();

					}
				} else if (filePath.endsWith("csv") || filePath.endsWith("txt")) {
					try {
						loadFile(file,table_name_staging,  list_field_name,  delimited,  idFile);
					} catch (Exception e) {
						e.printStackTrace();

					}
				} else {
					System.out.println("no method");
				}
			}
		} else {
			System.out.println("Không có bản ghi nào");
			return;
		}

//			Handle.convertDataFromStagingToWasehouse(String.valueOf(idFile));

		// sau khi đưa dữ liệu vào warehouse thì truncate bảng staging
//			truncateTable("STAGING", file_format_start_with);

	}
//phương thức tạo bảng nếu bảng chưa tồn tại
	public  void  createTable(String tableName,String list_field_name) throws SQLException {
		Connection connectDB = DBConnection.getConnection("STAGING");
		Statement st = connectDB.createStatement();
		StringTokenizer token = new StringTokenizer(list_field_name, "|");
		String sql="CREATE TABLE IF NOT EXISTS "+tableName+"(";
		int numberOfField=token.countTokens();
		for (int i = 0; i < numberOfField; i++) {
			
			sql+=token.nextToken()+" VARCHAR(250),";
		}
		sql=sql.substring(0, sql.length()-1);
		sql+=")";
		st.executeUpdate(sql);
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
		System.out.println("Update status ok");
		pre.close();

	}

	// phương thức dùng để update các trường của một dòng sau khi đưa vô staging
	// thành công
	public void updateFile(int id, int size, int total_row, int staging_load_row)
			throws SQLException, ClassNotFoundException {
		String sql = "Update logs set size=?,total_row=?,staging_load_row=?,time_staging=?,status_file=? where id=?";
		Connection con = DBConnection.getConnection("CONTROLDB");
		PreparedStatement pre = con.prepareStatement(sql);
		System.out.println(id);
		System.out.println(size);
		System.out.println(total_row);
		System.out.println(staging_load_row);
		pre.setInt(1, size);
		pre.setInt(2, total_row);
		pre.setInt(3, staging_load_row);
		pre.setDate(4, new Date(System.currentTimeMillis()));
		pre.setString(5, "OK STAGING");
		pre.setInt(6, id);
		pre.executeUpdate();
		System.out.println("Update file ok");
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

	public void loadFile(File file, String tableName, String list_field_name, String delimited, int id)
			throws SQLException, IOException, ClassNotFoundException {
	//tạo kết nối tới staging database
		Connection connect = DBConnection.getConnection("STAGING");
		//tạo bảng chứa dữ liệu nếu bảng không tồn tại
		createTable(tableName, list_field_name);
		BufferedReader lineReader = new BufferedReader(new FileReader(file));
		String lineText = "";
		//đọc bỏ dòng đầu tiên vì dòng đầu là tên các field
		lineText = lineReader.readLine();
		int total_row = 0, staging_load_row = 0, numberOfField = new StringTokenizer(list_field_name, "|").countTokens();
		//tạo câu sql để insert vào bảng tương ứng, bảng có x field thì có x dấu ?
		String sql = "Insert into " + tableName + " values (";
		while (numberOfField > 0) {
			if (numberOfField != 1) {
				sql += "?,";
			} else
				sql += "?";
			numberOfField--;
		}
		sql += ")";
		PreparedStatement pre = connect.prepareStatement(sql);
		numberOfField = new StringTokenizer(list_field_name, "|").countTokens();
		// xử lý các dòng còn lại trong file
		while ((lineText = lineReader.readLine()) != null) {
			System.out.println("lineText: " + lineText);
			// tách dòng theo delimited
			StringTokenizer st = new StringTokenizer(lineText, delimited);
			System.out.println("Count token: " + st.countTokens());
			//biến i dùng để pre.setString thứ i
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
			total_row++;
		}
		lineReader.close();
		updateFile(id, (int) file.length(), total_row, staging_load_row);
		
		
	}



	public void loadFileXSXL(File file, String tableName, String list_field_name, int id)
			throws SQLException, InvalidFormatException, IOException, ClassNotFoundException {
		//String filePath, String file_format_start_with, String tableName, int id
		int total_row=0;
		//đếm số field của bảng
		// nếu bảng chưa tồn tại thì tạo bảng
		createTable(tableName, list_field_name);
		//đếm tổng số trường của file này
		int numberOfField = new StringTokenizer(list_field_name, "|").countTokens();
		Connection connect = DBConnection.getConnection("STAGING");
		System.out.println("Connect DB Successfully");
		Workbook excel = new XSSFWorkbook(file);
		sheet = excel.getSheetAt(0);

		String sql = "Insert into " + tableName + " values (";
		while (numberOfField > 0) {
			if (numberOfField != 1) {
				sql += "?,";
			} else
				sql += "?";
			numberOfField--;
		}
		sql += ")";
System.out.println("Insert sql: "+ sql);
		PreparedStatement pre = connect.prepareStatement(sql);

		// get data from file and insert to table
//bỏ hàng 0 đọc từ hàng 1
		int i = 1;
		numberOfField = new StringTokenizer(list_field_name, "|").countTokens();
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
		updateFile(id, (int) file.length(), i-1, i-1);
		
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

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		try {

			new TranfertoStaging().loadFromSourceFile(3);

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
