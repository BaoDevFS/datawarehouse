package dao;

import java.util.ArrayList;

import model.SinhVien;

public class Handle {
	
	//convert data from staging to wasehouse
	public static void convertDataFromStagingToWasehouse(String idLogs) {
		
		//dem tong so phan tu cua staging va wasehouse
		//neu sau khi convert xog ma thieu bat ki dong du lieu nao thi xuat ra status ER nguoc lai la OK
		
		ArrayList<SinhVien> stagings = Staging.getAllSinhVien();
		ArrayList<SinhVien> wasehouses = Wasehouse.getAllSinhVien();
		int count = 0;
		for (SinhVien s : stagings) {
			for (SinhVien w : wasehouses) {
				if(!w.expired.equals("9999-12-30")) {
					count++;
					continue;
				}
				if(dataDuplicate(s, w)) {
					//cap nhat lai thoi gian va tao mot thang moi
					Wasehouse.updateData(w.id);
					break;
				}
			}
			count++;
			//update logs
			if(count == stagings.size()) {
				Logs.updateDataLogs(idLogs, "OK");
			}
			Wasehouse.insertData(s);
			
		}
		
	}
	
	// kiem tra du lieu trung lap
	public static boolean dataDuplicate(SinhVien a, SinhVien b) {
		if(a.mssv.equals(b.mssv)) {
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		convertDataFromStagingToWasehouse("2");
//		SinhVien st = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		SinhVien st1 = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		System.out.println(dataDuplicate(st, st1));
	}
	

}
