package mainprocess;

import java.io.IOException;
import java.sql.SQLException;

import part2.TranfertoStaging;

public class Tranform {
	public static void main(String[] args) throws NumberFormatException, ClassNotFoundException, SQLException, IOException {
		TranfertoStaging tranfertoStaging = new TranfertoStaging();
		tranfertoStaging.loadFromSourceFile(Integer.parseInt(args[0]));
	}
}
