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
	private String from_folder, download_to_dir_local;
	private Timer timer;
	SendMail sendMail;

	public CollectData() {
		sendMail = new SendMail();
	}

	public void startTask(int idRowConfig) {
		timer = new Timer();
		TimerTask timerTask = new TimerTask() {
			@Override
			public void run() {
				try {
					getConfig(idRowConfig);
				} catch (ClassNotFoundException | SQLException | IOException e) {
					e.printStackTrace();
				}
			}
		};
		timer.schedule(timerTask, 0, 30 * 1000);
	}

	public void getConfig(int i) throws ClassNotFoundException, SQLException, IOException {
		Connection connection = DBConnection.getConnection();
		String sql = "Select * from config";
		PreparedStatement pre = connection.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		int tmp = 0;
		while (rs.next()) {
			if (i == tmp) {
				System.out.println("Start tanks");
				id = rs.getInt("id");
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
				// login to server
				if (login(host, username, password)) {
					System.out.println("Login Success");
					// login
					getListFile(connection, host, from_folder, download_to_dir_local);
				} else {
					sendMail.sendEmail("Login fail", "Nguyennhubao999@gmail.com", "Login Fail");
				}
				rs.close();
				connection.close();
				System.out.println("End tanks");
				break;
			}
			tmp++;
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

	public void insertNewLog(Connection connection, int id_config, int id_group, String filename, String source_folder,
			String filetype_downdload, String status, String md5) throws ClassNotFoundException, SQLException {
		String sql = "Insert into logs (logs.id_config,logs.group_id,logs.status_file,logs.filename,logs.source_folder,logs.filetype_download,logs.time_download,logs.MD5) values(?,?,?,?,?,?,?,?)";
		PreparedStatement pre = connection.prepareStatement(sql);
		pre.setInt(1, id_config);
		pre.setInt(2, id_group);
		pre.setString(3, status);
		pre.setString(4, filename);
		pre.setString(5, source_folder);
		pre.setString(6, filetype_downdload);
		pre.setDate(7, new Date(System.currentTimeMillis()));
		pre.setString(8, md5);
		pre.execute();

	}

	// cập nhật thời gian download file
	public void updateLogs(Connection connection, int id, String status, String md5) throws SQLException {
		PreparedStatement pre = connection.prepareStatement(
				"update logs set time_download=now(), status_file='" + status + "',MD5='" + md5 + "' where id= " + id);
		pre.executeUpdate();
	}

	private int getGroupID(String name) {
		String id;
		try {
			id = name.substring(name.lastIndexOf(".") - 2, name.lastIndexOf("."));
			return Integer.parseInt(id);
		} catch (NumberFormatException e) {
			try {
				id = name.substring(name.lastIndexOf(".") - 1, name.lastIndexOf("."));
				return Integer.parseInt(id);
			} catch (NumberFormatException es) {
				throw es;
			}
		}
	}

	// kiểm tra file đã tồn tại hay chưa,
	// chưa thì trả về -1, tồn tại thì trả về id
	public ResultSet checkFileIsExitDB(Connection connection, int groupID, String fileName) throws SQLException {
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
			sendMail.sendEmail(e.toString(), "nguyennhubao999@gmail.com",
					"NoSuchAlgorithmException in getMD5FileLocal");
			return "";
		} catch (FileNotFoundException e) {
			sendMail.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "FileNotFoundException in getMD5FileLocal");
			return "";
		} catch (IOException e) {
			sendMail.sendEmail(e.toString(), "nguyennhubao999@gmail.com", "IOException in getMD5FileLocal");
			return "";
		}
	}

	public boolean getListFile(Connection connection, String host, String fromFolder, String download_to_dir_local) {
		try {
			// optional default is GET
			System.out.println(fromFolder);
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("api", "SYNO.FileStation.List");
			map.put("version", "1");
			map.put("method", "list");
			map.put("folder_path", fromFolder);
			map.put("_sid", sid);
			// get json list file
			String json = getJsonFromUrl(host + urlListFile, map);
			//
			Object obj = JSONValue.parse(json);
			JSONObject jsonObject = (JSONObject) obj;
			jsonObject = (JSONObject) jsonObject.get("data");
			JSONArray jsonArray = (JSONArray) jsonObject.get("files");
			String fileName, path;
			int groupId;
			for (int i = 0; i < jsonArray.size(); i++) {
				fileName = "";
				path = "";
				jsonObject = (JSONObject) jsonArray.get(i);
				fileName = (String) jsonObject.get("name");
				path = (String) jsonObject.get("path");
				if (!fileName.startsWith("sinhvien")) {
					continue;
				}
				if (!checkFileType(fileName)) {
					continue;
				}
				// get group id để kiểm tra
				try {
					groupId = getGroupID(fileName);
				} catch (NumberFormatException e) {
					continue;
				}
				// kiểm tra group id đã tồn tại hay chưa
				ResultSet rs = checkFileIsExitDB(connection, groupId, fileName);
				// file mới chưa download
				if (!rs.next()) {
					// dowload file
					if (downloadFile(host, fileName, path, download_to_dir_local)) {
						// get md5 local file
						String localMd5 = getMD5FileLocal(download_to_dir_local + fileName);
						// ghi log
						System.out.println("Download success " + fileName);
						insertNewLog(connection, id, groupId, fileName, fromFolder,
								fileName.substring(fileName.indexOf(".")), "ER", localMd5);
					} else {
						// download file thất bại insert log
						System.out.println("Download fail " + fileName);
						insertNewLog(connection, id, groupId, fileName, fromFolder,
								fileName.substring(fileName.indexOf(".")), "Download Error", "");
					}
					// file đã tồn tại kiểm tra update
				} else {
					// getStatusFile
					if (rs.getString("status_file").equals("Download Error")
							|| rs.getString("status_file").equals("Download Update")) {
						if (downloadFile(host, fileName, path, download_to_dir_local)) {
							// get md5 local file
							String localMd5 = getMD5FileLocal(download_to_dir_local + fileName);
							// cập nhật log
							System.out.println("ReDownlaod success " + fileName);
							updateLogs(connection, rs.getInt("id"), "ER", localMd5);
						} else {
							// download file thất bại update log
							System.out.println("ReDownlaod fail " + fileName);
							updateLogs(connection, rs.getInt("id"), "Download Error", "");

						}
					} else {

						// get md5 file in server
						String md5Sourc = getMD5File(host, path);
						// get md5 file in local
						String md5Local = getMD5FileLocal(download_to_dir_local + fileName);
						// kiểm tra lỗi md5 của file in local và md5 file trên server
						if (md5Local == "" || md5Sourc == null) {
							removelog(connection, rs.getInt("id"));
							continue;
						}
						// md5 giống nhau thì tiếp tục
						if (md5Local.equals(md5Sourc)) {
							System.out.println("File nothing change" + fileName);
							continue;
						} else {
							System.out.println("File is change" + fileName);
							// thay đổi trang thái file trong logs
							updateLogs(connection, rs.getInt("id"), "Download Update", "");
						}
					}
				}
				rs.close();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;

	}

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
			httpClient = (HttpURLConnection) new URL(url).openConnection();

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
		String taskid = getChecksum_TaskID(host, file_path);
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.FileStation.MD5");
		map.put("version", "1");
		map.put("method", "status");
		map.put("taskid", taskid);
		map.put("_sid", sid);
		String json = getJsonFromUrl(host + urlListFile, map);
		Object obj = JSONValue.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		jsonObject = (JSONObject) jsonObject.get("data");
		return (String) jsonObject.get("md5");

	}

	private String getChecksum_TaskID(String host, String file_path) {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.FileStation.MD5");
		map.put("version", "1");
		map.put("method", "start");
		map.put("file_path", file_path);
		map.put("_sid", sid);
		String json = getJsonFromUrl(host + urlListFile, map);
		Object obj = JSONValue.parse(json);
		JSONObject jsonObject = (JSONObject) obj;
		jsonObject = (JSONObject) jsonObject.get("data");
		return "\"" + (String) jsonObject.get("taskid") + "\"";
	}

	private String getPostDataString(HashMap<String, String> params) throws UnsupportedEncodingException {
		StringBuilder result = new StringBuilder();
		boolean first = true;
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
			httpClient = (HttpURLConnection) new URL(host + urlListFile).openConnection();

			HashMap<String, String> map = new HashMap<String, String>();
			map.put("api", "SYNO.FileStation.Download");
			map.put("version", "1");
			map.put("method", "download");
			map.put("path", path);
			map.put("_sid", sid);
			map.put("mode", "open");
			httpClient.setRequestMethod("GET");
			httpClient.setDoInput(true);
			httpClient.setDoOutput(true);
			// add request header
			OutputStream os = httpClient.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.write(getPostDataString(map));
			writer.flush();
			writer.close();
			os.close();
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder_tmp + fileName));
			BufferedInputStream bis = new BufferedInputStream(httpClient.getInputStream());
			byte[] arr = new byte[2048];
			int index;
			while ((index = bis.read(arr)) != -1) {
				bos.write(arr, 0, index);
				bos.flush();
			}
			bos.close();
			bis.close();
			return true;
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
		collectData.startTask(0);
		
//		collectData.login("http://drive.ecepvn.org:5000/", "guest_access", "123456");
//		collectData.getMD5File("http://drive.ecepvn.org:5000/", "/ECEP/song.nguyen/DW_2020/data/sinhvien_chieu_nhom16.xlsx");
//		System.out.println(collectData.getMD5FileLocal("D:\\00_HK2_3\\DataWarehouse\\sinhvien_chieu_nhom16.xlsx"));

//		collectData.getConfig();
//		System.out.println(collectData.getGroupID("sinhvien_chieu_nhom6.xlsx"));
//		collectData.insertLog(1, "", "", ".xlsx");
	}
}
