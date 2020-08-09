package mainprocess;

import java.io.IOException;
import java.sql.SQLException;

import getdata.CollectData;

public class DownLoad {
	public static void main(String[] args) throws NumberFormatException, ClassNotFoundException, SQLException, IOException {
		CollectData collect = new CollectData();
		collect.getConfig(Integer.parseInt(args[0]));
	}
}
