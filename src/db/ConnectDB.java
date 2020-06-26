package db;

import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectDB {
	private static ConnectDB connectDB;
    static Connection con;
    String url;
    String user;
    String password;
    private ConnectDB(String url, String user, String password){
        this.url = url;
        this.user = user;
        this.password = password;
        try{
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);
            System.out.println("success connect");
        }catch(Exception e){
            System.out.println("error connect");
            System.out.println(e.getMessage());
        }
    }
    public static Connection getConnection(String url,String user, String password){
        if(connectDB == null){
            connectDB = new ConnectDB(url,user,password);
        }
        return con;
    }
//    public static void main(String[] args) {
    	
//    	String url = "jdbc:mysql://localhost:3306/controldb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
//    	String user = "root";
//    	String password = "";
//    	con = ConnectDB.getConnection(url, user, password);
//    	
//	}

}
