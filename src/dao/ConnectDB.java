package dao;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectDB {
	private static ConnectDB connectDB;
    static Connection con;
    String url = "jdbc:mysql://localhost:3306/wasehouse?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	String user = "root";
	String password = "";
    private ConnectDB(){
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);
//            System.out.println("success connect");
        }catch(Exception e){
//            System.out.println("error connect");
            System.out.println(e.getMessage());
        }
    }
    public static Connection getConnection(){
        if(connectDB == null){
            connectDB = new ConnectDB();
        }
        return con;
    }
    public static Connection getConnectionNew() {
    
    	return null;
    }
//    public static void main(String[] args) {
    	
//    	String url = "jdbc:mysql://localhost:3306/controldb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
//    	String user = "root";
//    	String password = "";
//    	con = ConnectDB.getConnection(url, user, password);
//    	
//	}

}
