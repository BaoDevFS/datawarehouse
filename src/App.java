import java.util.ArrayList;

import staging.ProcessStaging;
import wasehouse.ProcessWasehouse;

public class App {
	public static void main(String[] args) {
		String url = "jdbc:mysql://localhost:3306/wasehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    	String user = "root";
    	String password = "";
		ProcessStaging processStaging = new ProcessStaging();
		ArrayList<String> result = processStaging.getData(url, user, password);
		System.out.println(result);
		
		String urlStaging = "jdbc:mysql://localhost:3306/wasehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		ProcessWasehouse processWasehouse = new ProcessWasehouse();
		processWasehouse.getData(url, user, password, urlStaging);
	}

}
