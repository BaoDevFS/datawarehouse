package getdata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import db.DBConnection;
import mail.SendMail;

public class CollectData {
	private String urlLogin = "/webapi/auth.cgi";
	private String urlListFile = "/webapi/entry.cgi";
	private String sid = "";
	private int id;
	private String from_folder, download_to_dir_local, file_format_start_with, file_format_define_group;
	private Timer timer;
	SendMail sendMail;
	ArrayList<String> listPathFile;
	ArrayList<String> listFileName;

	public CollectData() {
		sendMail = new SendMail();
	}

	public void getConfig(int id) throws ClassNotFoundException, IOException, SQLException {
		// kết nối db control
		Connection connection = DBConnection.getConnection("CONTROLDB");
		// kiểm tra lỗi thì thông báo mail
		if (connection == null) {
			sendMail.sendEmail("Khong ket noi được CONTROLDB", "guyennhubao999@gmail.com",
					"CONNECT TO CONTROLDB ERROR");
			return;
		}
		// query lấy row Config theo id
		String sql = "Select * from config where id =" + id;
		PreparedStatement pre = connection.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		// duyêt ResultSet lấy các thông số
		if (rs.next()) {
			System.out.println("Start tanks");
			this.id = id;
			System.out.println("id: " + id);
			String host = rs.getString("ip_address");
			System.out.println("host: " + host);
			String username = rs.getString("username");
			System.out.println("username: " + username);
			String password = rs.getString("password");
			System.out.println("password: " + password);
			from_folder = rs.getString("download_from_folder");
			System.out.println("from_folder: " + from_folder);
			download_to_dir_local = rs.getString("download_to_dir_local");
			System.out.println("download_to_dir_local: " + download_to_dir_local);
			file_format_start_with = rs.getString("file_format_start_with");
			file_format_define_group = rs.getString("file_format_define_group");
			// login to server
			if (login(host, username, password)) {
				System.out.println("Login Success");
				// login
				getListFile(connection, host, from_folder, download_to_dir_local);
				checkStatusFileInSystem(connection, host, from_folder, download_to_dir_local);
			} else {
				System.out.println("Login Fail");
				sendMail.sendEmail("Login fail to server", "Nguyennhubao999@gmail.com", "Login Fail");
			}
			rs.close();
			connection.close();
			System.out.println("End tanks");
		}

	}

	public boolean login(String host, String username, String password) throws IOException {
		// optional default is GET
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.API.Auth");
		map.put("version", "3");
		map.put("method", "login");
		map.put("account", username);
		map.put("passwd", password);
		map.put("session", "FileStation");
		map.put("format", "cookie");
		String json = getJsonFromUrl(host + urlLogin, map);
		Object object = JSONValue.parse(json);
		JSONObject jsonObject = (JSONObject) object;
		try {
			jsonObject = (JSONObject) jsonObject.get("data");
			sid = (String) jsonObject.get("sid");
		} catch (NullPointerException e) {
			return false;
		}
		if (json.contains("sid")) {
			return true;
		} else {
			return false;
		}

	}
	// chèn 1 dòng log mới vào table logs
	public void insertNewLog(Connection connection, int id_config, int id_group, String filename, String source_folder,
			String filetype_downdload, String status, String md5) {
		String sql = "Insert into logs (logs.id_config,logs.group_id,logs.status_file,logs.filename,logs.source_folder,logs.filetype_download,logs.time_download,logs.MD5) values(?,?,?,?,?,?,?,?)";
		PreparedStatement pre;
		try {
			pre = connection.prepareStatement(sql);
			pre.setInt(1, id_config);
			pre.setInt(2, id_group);
			pre.setString(3, status);
			pre.setString(4, filename);
			pre.setString(5, source_folder);
			pre.setString(6, filetype_downdload);
			pre.setDate(7, new Date(System.currentTimeMillis()));
			pre.setString(8, md5);
			pre.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			sendMail.sendEmail("Insert new log error:" + "\n" + e.toString(), "nguyennhubao999@gmail.com",
					"Download Error");
		}

	}

	// cập nhật thời gian download file
	public void updateLogs(Connection connection, int id, String status, String md5) {
		PreparedStatement pre;
		try {
			pre = connection.prepareStatement("update logs set time_download=now(), status_file='" + status + "',MD5='"
					+ md5 + "' where id= " + id);
			pre.executeUpdate();
		} catch (SQLException e) {
			sendMail.sendEmail("UpdateLog Error in id:" + id + "\n" + e.toString(), "nguyennhubao999@gmail.com",
					"UpdateLog Error");
			e.printStackTrace();
		}

	}
	// tách groupID từ file name
	private int getGroupID(String name) {
		String id;
		try {
			id = name.substring(name.lastIndexOf(file_format_define_group) - 2,
					name.lastIndexOf(file_format_define_group));
			return Integer.parseInt(id);
		} catch (NumberFormatException e) {
			try {
				id = name.substring(name.lastIndexOf(file_format_define_group) - 1,
						name.lastIndexOf(file_format_define_group));
				return Integer.parseInt(id);
			} catch (NumberFormatException es) {
				throw es;
			}
		}
	}

	// kiểm tra file đã tồn tại hay chưa,
	// chưa thì trả về -1, tồn tại thì trả về id
	public ResultSet checkFileIsExitDB(Connection connection, int groupID, String fileName) throws SQLException {
		// query lấy dòng log trong table logs 
		PreparedStatement pre = connection.prepareStatement("Select * from logs where group_id=? and filename=?");
		pre.setInt(1, groupID);
		pre.setString(2, fileName);
		return pre.executeQuery();

	}

	public String getMD5FileLocal(String pathFile) {
		try {
			MessageDigest md5Digest = MessageDigest.getInstance("MD5");

			// Get file input stream for reading the file content
			FileInputStream fis = new FileInputStream(pathFile);

			// Create byte array to read data in chunks
			byte[] byteArray = new byte[1024];
			int bytesCount = 0;

			// Read file data and update in message digest
			while ((bytesCount = fis.read(byteArray)) != -1) {
				md5Digest.update(byteArray, 0, bytesCount);
			}
			;

			// close the stream; We don't need it now.
			fis.close();

			// Get the hash's bytes
			byte[] bytes = md5Digest.digest();

			// This bytes[] has bytes in decimal format;
			// Convert it to hexadecimal format
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < bytes.length; i++) {
				sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
			}

			// return complete hash
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			return "";
		} catch (FileNotFoundException e) {
			return "";
		} catch (IOException e) {
			return "";
		}
	}

	private void processFileNotExitsInSystem(Connection connection, String host, String fileName, String pathFile,
			int groupId, String fromFolder) {
		// dowload file
		if (downloadFile(host, fileName, pathFile, download_to_dir_local)) {
			// get md5 local file
			String localMd5 = getMD5FileLocal(download_to_dir_local + fileName);
			// ghi log
			System.out.println("Download success " + fileName);
			insertNewLog(connection, id, groupId, fileName, fromFolder, fileName.substring(fileName.indexOf(".")), "ER",
					localMd5);
		} else {
			// download file thất bại insert log
			System.out.println("Download fail " + fileName);
			insertNewLog(connection, id, groupId, fileName, fromFolder, fileName.substring(fileName.indexOf(".")),
					"Download Error", "");
		}
	}

	private String getMd5FromLog(Connection connection, int id) {
		// query lấy trường md5 trong dòng log có id 
		String sql = "Select MD5 from logs where id=" + id;
		try {
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			ResultSet rs = preparedStatement.executeQuery();
			// nếu có thì trả về string md5
			if (rs.next()) {
				return rs.getString(1);
				// không thì trả về chuỗi rỗng
			} else {
				return "";
			}
		} catch (SQLException e) {
			return "";
		}
	}

	private void processFileExitsInSystem(Connection connection, String host, String fileName, String pathFile,
			String statusFile, int id) {
		// kiểm tra trạng thái file có giống vs các trạng thái lỗi hay k
		if (statusFile.equals("Download Error") || statusFile.equals("Download Update")
				|| statusFile.equals("FILE_NOT_FOUND")) {
			if (downloadFile(host, fileName, pathFile, download_to_dir_local)) {
				// get md5 local file
				String localMd5 = getMD5FileLocal(download_to_dir_local + fileName);
				// cập nhật log
				System.out.println("ReDownlaod success " + fileName);
				updateLogs(connection, id, "ER", localMd5);
			} else {
				// download file thất bại update log
				System.out.println("ReDownlaod fail " + fileName);
				updateLogs(connection, id, "Download Error", "");

			}
			// kiểm tra md5 của file trên local với file trên server
		} else {
			// get md5 file in server
			String md5Sourc = getMD5File(host, pathFile);
			// get md5 file in local
			String md5Local = getMd5FromLog(connection, id);
			// kiểm tra lỗi md5 của file in local và md5 file trên server
			if (md5Local == "" || md5Sourc == null) {
				removelog(connection, id);
				return;
			}
			// md5 giống nhau thì tiếp tục
			if (md5Local.equals(md5Sourc)) {
				System.out.println("File nothing change: " + fileName);
			} else {
				System.out.println("File is change: " + fileName);
				// thay đổi trang thái file trong log thành  Download Update để down lại
				updateLogs(connection, id, "Download Update", "");
			}
		}
	}

	// kiểm tra trạng thái file trong hệ thống
	public void checkStatusFileInSystem(Connection connection, String host, String fromFolder,
			String download_to_dir_local) throws SQLException {
		String fileName, pathFile;
		int groupId;
		for (int i = 0; i < listFileName.size(); i++) {
			fileName = listFileName.get(i);
			pathFile = listPathFile.get(i);
			// get group id để kiểm tra
			try {
				groupId = getGroupID(fileName);
			} catch (NumberFormatException e) {
				continue;
			}
			// kiểm tra group id đã tồn tại hay chưa
			ResultSet rs = checkFileIsExitDB(connection, groupId, fileName);
			// k tồn tại trong hệ thống
			if (!rs.next()) {
				processFileNotExitsInSystem(connection, host, fileName, pathFile, groupId, fromFolder);
			} else {
				processFileExitsInSystem(connection, host, fileName, pathFile, rs.getString("status_file"),
						rs.getInt("id"));
			}
			rs.close();
		}
	}

	public boolean getListFile(Connection connection, String host, String fromFolder, String download_to_dir_local) {
		// optional default is GET
		listPathFile = new ArrayList<String>();
		listFileName = new ArrayList<String>();
		System.out.println(fromFolder);
		// những parameter mà header phải có để lấy đc list file
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.FileStation.List");
		map.put("version", "1");
		map.put("method", "list");
		map.put("folder_path", fromFolder);
		map.put("_sid", sid);
		// get json list file
		String json = getJsonFromUrl(host + urlListFile, map);
		// dùng thư viện để bóc tách json trả về
		Object obj = JSONValue.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		jsonObject = (JSONObject) jsonObject.get("data");
		JSONArray jsonArray = (JSONArray) jsonObject.get("files");
		String fileName, path;
		// duyệt for để lấy từng dòng filename và path
		for (int i = 0; i < jsonArray.size(); i++) {
			fileName = "";
			path = "";
			jsonObject = (JSONObject) jsonArray.get(i);
			fileName = (String) jsonObject.get("name");
			path = (String) jsonObject.get("path");
			// nếu filename k bắt đầu bằng chuỗi start_with đã cho thì bỏ qua
			if (!fileName.startsWith(file_format_start_with)) {
				continue;
			}
			// kiểm tra dịnh đạng file
			// không đúng thì bỏ qua
			if (!checkFileType(fileName)) {
				continue;
			}
			// add fileName và pathFile và arraylist
			listFileName.add(fileName);
			listPathFile.add(path);
		}
		return true;

	}
	// xóa log với id truyền vào
	public void removelog(Connection connection, int id) {
		String sql = "Delete from logs where id=" + id;
		PreparedStatement preparedStatement;
		try {
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String getJsonFromUrl(String url, HashMap<String, String> param) {
		HttpURLConnection httpClient;
		try {
			// mở kêt nối tới url
			httpClient = (HttpURLConnection) new URL(url).openConnection();
			// thêm các option
			httpClient.setRequestMethod("GET");
			httpClient.setDoInput(true);
			httpClient.setDoOutput(true);
			// add request header
			OutputStream os = httpClient.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.write(getPostDataString(param));
			writer.flush();
			writer.close();
			os.close();
			// lưu lại các json trả về vào tringbuffer
			BufferedReader in = new BufferedReader(new InputStreamReader(httpClient.getInputStream()));
			StringBuilder response = new StringBuilder();
			String line;
			while ((line = in.readLine()) != null) {
				response.append(line);
			}
			httpClient.disconnect();
			return response.toString();
		} catch (IOException e) {
			e.printStackTrace();
			return "";
		}
	}

	private String getMD5File(String host, String file_path) {
		//
		String taskid = getChecksum_TaskID(host, file_path);
		// header cần phải có để lấy dc md5
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.FileStation.MD5");
		map.put("version", "1");
		map.put("method", "status");
		map.put("taskid", taskid);
		map.put("_sid", sid);
		// lấy json trả về
		String json = getJsonFromUrl(host + urlListFile, map);
		// bóc tách json
		Object obj = JSONValue.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		jsonObject = (JSONObject) jsonObject.get("data");
		// trả về md5
		return (String) jsonObject.get("md5");

	}

	private String getChecksum_TaskID(String host, String file_path) {
		//header cần phải có để lấy tackid ở server
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.FileStation.MD5");
		map.put("version", "1");
		map.put("method", "start");
		map.put("file_path", file_path);
		map.put("_sid", sid);
		// bóc tách json đểlaays
		String json = getJsonFromUrl(host + urlListFile, map);
		Object obj = JSONValue.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		jsonObject = (JSONObject) jsonObject.get("data");
		// thêm kí tự đặc biệt cho task id để gửi lên server tiếp theo
		return "\"" + (String) jsonObject.get("taskid") + "\"";
	}
	// chuyển hashmap sang string dể gửi lên server
	private String getPostDataString(HashMap<String, String> params) throws UnsupportedEncodingException {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		// duyệt từng phần tử trong hashmap
		for (Map.Entry<String, String> entry : params.entrySet()) {
			if (first)
				first = false;
			else
				result.append("&");

			result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
			result.append("=");
			result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
		}

		return result.toString();
	}
	// kiểm tra định dạng file
	private boolean checkFileType(String fileName) {
		if (!fileName.endsWith(".xlsx")) {
			if (!fileName.endsWith(".csv")) {
				if (!fileName.endsWith(".txt")) {
					return false;
				}
			}
		}
		return true;
	}

	public boolean downloadFile(String host, String fileName, String path, String folder_tmp) {
		HttpURLConnection httpClient;
		try {
			// mở kết nối tới server theo url
			httpClient = (HttpURLConnection) new URL(host + urlListFile).openConnection();
			// header của kết nối cần phải có
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("api", "SYNO.FileStation.Download");
			map.put("version", "1");
			map.put("method", "download");
			map.put("path", path);
			map.put("_sid", sid);
			map.put("mode", "open");
			// set phương thức của nó là get
			httpClient.setRequestMethod("GET");
			// cho phép thêm dữ liệu vào và lấy dữ liệu ra
			httpClient.setDoInput(true);
			httpClient.setDoOutput(true);
			// add request header
			OutputStream os = httpClient.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.write(getPostDataString(map));
			writer.flush();
			writer.close();
			os.close();
			// mở kết nối file để lưu file
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder_tmp + fileName));
			BufferedInputStream bis = new BufferedInputStream(httpClient.getInputStream());
			byte[] arr = new byte[2048];
			int index;
			// đọc mảng byte từ response và write vào file
			while ((index = bis.read(arr)) != -1) {
				bos.write(arr, 0, index);
				bos.flush();
			}
			// dóng kết nỗi
			bos.close();
			bis.close();
			return true;
			// nếu lỗi gửi email
		} catch (IOException e) {
			System.out.println("Error download file" + fileName);
			sendMail.sendEmail("Download error file:" + fileName + "\n" + e.toString(), "nguyennhubao999@gmail.com",
					"Download Error");
			return false;
		}
	}

	public void convertSelectedSheetInXLXSFileToCSV(File xlsxFile, int sheetIdx) throws Exception {
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
					sb.append((int) cell.getNumericCellValue());
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

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		CollectData collectData = new CollectData();
//		collectData.startTask(3);

//		collectData.login("http://drive.ecepvn.org:5000/", "guest_access", "123456");
//		collectData.getMD5File("http://drive.ecepvn.org:5000/", "/ECEP/song.nguyen/DW_2020/data/sinhvien_chieu_nhom16.xlsx");
//		System.out.println(collectData.getMD5FileLocal("D:\\00_HK2_3\\DataWarehouse\\sinhvien_chieu_nhom16.xlsx"));

//		collectData.getConfig();
//		System.out.println(collectData.getGroupID("sinhvien_chieu_nhom6.xlsx"));
//		collectData.insertLog(1, "", "", ".xlsx");
	}
}
