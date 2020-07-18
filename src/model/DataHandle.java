package model;

import java.util.Date;
import java.util.StringTokenizer;
import java.util.UUID;
import java.text.SimpleDateFormat;

public class DataHandle {
	
	
	public static java.sql.Date getDate() {
		Date now = new Date();
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        String mysqlDateString = formatter.format(now);
        //handle
        StringTokenizer st = new StringTokenizer(mysqlDateString,"-");
//        Date d = new java.sql.Date(Integer.parseInt(st.nextToken())-1900, Integer.parseInt(st.nextToken())-1,Integer.parseInt(st.nextToken()));
        
        return new java.sql.Date(Integer.parseInt(st.nextToken())-1900, Integer.parseInt(st.nextToken())-1,Integer.parseInt(st.nextToken()));
	}
	public static String setId() {
		return UUID.randomUUID().toString();
	}
	
	public static void main(String[] args) {
//		System.out.println(getDate());
	}
}
