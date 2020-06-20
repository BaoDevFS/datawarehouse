package getdata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class CollectData {
	String urlLogin = "http://drive.ecepvn.org:5000/webapi/auth.cgi";
	String urlListFile = "http://drive.ecepvn.org:5000/webapi/entry.cgi";
	String sid = "eJdJYEvG1i2eM1130LWN029292";
	String folder = "D:/xampp/mysql/data/datawarehouse/data/";

	public void login() throws IOException {

		HttpURLConnection httpClient = (HttpURLConnection) new URL(urlLogin).openConnection();
		// optional default is GET
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.API.Auth");
		map.put("version", "3");
		map.put("method", "login");
		map.put("account", "guest_access");
		map.put("passwd", "123456");
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
		}

	}

	public void getListFile() throws MalformedURLException, IOException {
		HttpURLConnection httpClient = (HttpURLConnection) new URL(urlListFile).openConnection();
		// optional default is GET
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("api", "SYNO.FileStation.List");
		map.put("version", "1");
		map.put("method", "list");
		map.put("folder_path", "/ECEP/song.nguyen/DW_2020/data");
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
		try (BufferedReader in = new BufferedReader(new InputStreamReader(httpClient.getInputStream()))) {

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
			for (int i = 0; i < jsonArray.size(); i++) {
				jsonObject = (JSONObject) jsonArray.get(i);
				System.out.println(jsonObject.get("name"));
				System.out.println(jsonObject.get("path"));
				if(((String)jsonObject.get("path")).endsWith(".xlsx")||((String)jsonObject.get("path")).endsWith(".csv")||((String)jsonObject.get("path")).endsWith(".txt"))
				System.out.println(downloadFile((String)jsonObject.get("name"), (String)jsonObject.get("path")));
			}
		}

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

	public boolean downloadFile(String fileName, String path) {
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
				System.out.println("cop");
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
		CollectData collectData = new CollectData();
		try {
			collectData.login();
			collectData.getListFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
