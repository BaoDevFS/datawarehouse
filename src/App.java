import wasehouse.ProcessWasehouse;

public class App {
	public static void main(String[] args) {
		String url = "jdbc:mysql://localhost:3306/wasehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		
//		ProcessStaging processStaging = new ProcessStaging();
//		ArrayList<String> result = processStaging.getData();
//		System.out.println(result);
		
		ProcessWasehouse processWasehouse = new ProcessWasehouse();
//		processWasehouse.transport();
//		
		processWasehouse.testDuplicateData();
//		processWasehouse.test();
		
	}

}
