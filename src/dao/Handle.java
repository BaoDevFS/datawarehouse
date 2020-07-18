package dao;

import java.util.ArrayList;

import model.SinhVien;

public class Handle {
	
	//convert data from staging to wasehouse
	public static void convertDataFromStagingToWasehouse() {
		
		ArrayList<SinhVien> stagings = Staging.getAllSinhVien();
		ArrayList<SinhVien> wasehouses = Wasehouse.getAllSinhVien();
		
		System.out.println("staging:\n" + stagings.toString());
		System.out.println("wasehouse:\n" + wasehouses.toString());
		
		for (SinhVien s : stagings) {
			for (SinhVien w : wasehouses) {
				if(w.expired.equals("0001-12-30")) {
					continue;
				}
				if(dataDuplicate(s, w)) {
					//cap nhat lai thoi gian va tao mot thang moi
					Wasehouse.updateData(w.id);
					break;
				}
			}
			//tao mot thang moi
			Wasehouse.insertData(s);
			
		}
		
	}
	
	// kiem tra du lieu trung lap
	public static boolean dataDuplicate(SinhVien a, SinhVien b) {
		if(a.stt.equals(b.stt) && b.expired.equals("9999-12-30")) {
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		convertDataFromStagingToWasehouse();
//		SinhVien st = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		SinhVien st1 = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		System.out.println(dataDuplicate(st, st1));
	}
	

}
