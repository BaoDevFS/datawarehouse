package dao;

import java.util.ArrayList;

import model.SinhVien;

public class Handle {
	
	//convert data from staging to wasehouse
	public static void convertDataFromStagingToWasehouse(String idLogs) {
		ArrayList<SinhVien> stagings = Staging.getAllSinhVien();
		SinhVien sv;
		for (SinhVien sinhVien : stagings) {
			sv = Wasehouse.getOneSinhVien(sinhVien.mssv);
			if(sv != null) {
				if(!isDataDuplicate(sinhVien, sv)) {
					Wasehouse.updateData(Integer.parseInt(sv.id));
					Wasehouse.insertData(sinhVien);
				}
				continue;
			}else {
				Wasehouse.insertData(sinhVien);
			}
		}
		
	}
	
	// kiem tra du lieu trung lap
	public static boolean isDataDuplicate(SinhVien s, SinhVien w) {
			if(s.lastName.equals(w.lastName) && s.firstName.equals(w.firstName) && s.dayBorn.equals(w.dayBorn) && s.classId.equals(w.classId)
					&& s.className.equals(w.className) && s.phoneNumber.equals(w.phoneNumber) && s.email.equals(w.email) && s.address.equals(w.address) && s.note.equals(w.note)) {
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
