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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
	String host, from_folder, download_to_folder_tmp, folder_destinati;

	public void getConfig() throws ClassNotFoundException, SQLException, IOException {
		Connection connection = DBConnection.getConnection();
		String sql = "Select * from config";
		PreparedStatement pre = connection.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		while (rs.next()) {
			int id = rs.getInt("id");
			String host = rs.getString("ip_address");
			String username = rs.getString("username");
			String password = rs.getString("password");
			from_folder = rs.getString("download_from_folder");
			download_to_folder_tmp = rs.getString("download_to_dir_local");
			folder_destinati = rs.getString("dir_local_destination");
			if (login(host, username, password)) {
				if (getListFile(host, from_folder, folder_destinati)) {

				}
			}
		}
	}

	public boolean login(String host, String username, String password) throws IOException {
		HttpURLConnection httpClient = (HttpURLConnection) new URL(host + urlLogin).openConnection();
		// optional default is GET
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.API.Auth");
		map.put("version", "3");
		map.put("method", "login");
		map.put("account", username);
		map.put("passwd", password);
		map.put("session", "FileStation");
		map.put("format", "cookie");
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
		int responseCode = httpClient.getResponseCode();
		System.out.println("Response Code : " + responseCode);
		try (BufferedReader in = new BufferedReader(new InputStreamReader(httpClient.getInputStream()))) {

			StringBuilder response = new StringBuilder();
			String line;

			while ((line = in.readLine()) != null) {
				response.append(line);
			}
			// print result
			System.out.println(response.toString());
			line = response.substring(response.lastIndexOf("{"), response.indexOf("}"));
			sid = line.substring(line.indexOf(":") + 2, line.length() - 1);
			System.out.println(sid);
			if (line.contains("sid")) {
				return true;
			} else {
				return false;
			}
		}

	}

	public boolean getListFile(String host, String fromFolder, String folder_tmp) {
		try {
			HttpURLConnection httpClient = (HttpURLConnection) new URL(host + urlListFile).openConnection();
			// optional default is GET
			HashMap<String, String> map = new HashMap<String, String>();
			map.put("api", "SYNO.FileStation.List");
			map.put("version", "1");
			map.put("method", "list");
			map.put("folder_path", fromFolder);
			map.put("_sid", sid);
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
			int responseCode = httpClient.getResponseCode();
			System.out.println("Response Code : " + responseCode);
			BufferedReader in = new BufferedReader(new InputStreamReader(httpClient.getInputStream()));
			StringBuilder response = new StringBuilder();
			String line;

			while ((line = in.readLine()) != null) {
				response.append(line);
			}
			// print result
			System.out.println(response.toString());
			Object obj = JSONValue.parse(response.toString());
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
				if (!name.startsWith("sinvien")) {
					continue;
				}
				System.out.println(name);
				System.out.println(path);
				if (path.endsWith(".xlsx") || path.endsWith(".csv") || path.endsWith(".txt"))
					downloadFile((String) jsonObject.get("name"), (String) jsonObject.get("path"), folder_tmp);
			}
		} catch (ProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
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

	public boolean downloadFile(String fileName, String path, String folder_tmp) {
		HttpURLConnection httpClient;
		try {
			httpClient = (HttpURLConnection) new URL(urlListFile).openConnection();

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
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder + fileName));
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
			return false;
		}
	}

	public static void main(String[] args) {
//		CollectData collectData = new CollectData();
//		try {
//			collectData.login();
//			collectData.getListFile();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
