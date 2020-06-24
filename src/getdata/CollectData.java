package getdata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.text.SimpleAttributeSet;

import org.apache.poi.hssf.record.DBCellRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import db.DBConnection;

public class CollectData {
	String urlLogin = "/webapi/auth.cgi";
	String urlListFile = "/webapi/entry.cgi";
	String sid = "";
	String folder = "D:/xampp/mysql/data/datawarehouse/data/";
	int id;
	String from_folder, download_to_dir_local, folder_destinati;
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	public void getConfig() throws ClassNotFoundException, SQLException, IOException {
		Connection connection = DBConnection.getConnection();
		String sql = "Select * from config";
		PreparedStatement pre = connection.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		while (rs.next()) {
			id = rs.getInt("id");
			System.out.println(id);
			String host = rs.getString("ip_address");
			System.out.println(host);
			String username = rs.getString("username");
			System.out.println(username);
			String password = rs.getString("password");
			System.out.println(password);
			from_folder = rs.getString("download_from_folder");
			System.out.println(from_folder);
			download_to_dir_local = rs.getString("download_to_dir_local");
			System.out.println(download_to_dir_local);
			folder_destinati = rs.getString("dir_local_destination");
			System.out.println(folder_destinati);
			if (login(host, "", password)) {
//				getMD5File(host, "/ECEP/song.nguyen/DW_2020/data/sinvien_chieu_nhom16.xlsx");
				getListFile(connection, host, from_folder, download_to_dir_local);
			}
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

	public void insertLog(Connection connection, int id_config, String filename, String source_folder,
			String filetype_downdload, String status) throws ClassNotFoundException, SQLException {
		String sql = "Insert into logs (logs.id_config,logs.status_file,logs.filename,logs.source_folder,logs.filetype_download,logs.time_download) values(?,?,?,?,?,?)";
		PreparedStatement pre = connection.prepareStatement(sql);
		pre.setInt(1, id_config);
		pre.setString(2, status);
		pre.setString(3, filename);
		pre.setString(4, source_folder);
		pre.setString(5, filetype_downdload);
		pre.setDate(6, new Date(System.currentTimeMillis()));
		pre.execute();

	}

	public boolean getListFile(Connection connection, String host, String fromFolder, String folder_tmp) {
		try {
			// optional default is GET
			System.out.println(fromFolder);
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("api", "SYNO.FileStation.List");
			map.put("version", "1");
			map.put("method", "list");
			map.put("folder_path", fromFolder);
			map.put("_sid", sid);
			
			String json = getJsonFromUrl(host + urlListFile, map);
			
			Object obj = JSONValue.parse(json);
			JSONObject jsonObject = (JSONObject) obj;
			jsonObject = (JSONObject) jsonObject.get("data");
			JSONArray jsonArray = (JSONArray) jsonObject.get("files");
			String name, path;
			for (int i = 0; i < jsonArray.size(); i++) {
				name = "";
				path = "";
				jsonObject = (JSONObject) jsonArray.get(i);
				name = (String) jsonObject.get("name");
				path = (String) jsonObject.get("path");
				if (!name.startsWith("sinhvien")) {
					continue;
				}
				if (name.endsWith(".xlsx") || name.endsWith(".csv") || name.endsWith(".txt")) {
					if (downloadFile(host, name, path, folder_tmp)) {
						insertLog(connection, id, name, fromFolder, name.substring(name.indexOf(".")), "ER");
					} else {
						insertLog(connection, id, name, fromFolder, name.substring(name.indexOf(".")),
								"Download Error");
					}

				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;

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
			int responseCode = httpClient.getResponseCode();
			System.out.println("Response Code : " + responseCode);
			BufferedReader in = new BufferedReader(new InputStreamReader(httpClient.getInputStream()));
			StringBuilder response = new StringBuilder();
			String line;
			while ((line = in.readLine()) != null) {
				response.append(line);
			}
			System.out.println(response.toString());
			httpClient.disconnect();
			return response.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		System.out.println((String) jsonObject.get("md5"));
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
		System.out.println((String) jsonObject.get("taskid"));
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
			// TODO Auto-generated catch block
			System.out.println("Error download file" + fileName);
			e.printStackTrace();
			return false;
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		CollectData collectData = new CollectData();
		collectData.getConfig();
//		collectData.insertLog(1, "", "", ".xlsx");
	}
}
