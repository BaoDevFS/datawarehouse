package dao;

import java.util.ArrayList;

import model.LopHoc;
import model.MonHoc;
import model.SinhVien;

public class Handle {
	
	//convert data from staging to wasehouse(sinh vien)
	public static void convertDataSinhVienFromStagingToWasehouse(String idLogs) {
		ArrayList<SinhVien> stagings = Staging.getAllSinhVien();
		SinhVien sv;
		for (SinhVien sinhVien : stagings) {
			sv = Wasehouse.getOneSinhVien(sinhVien.mssv);
			if(sv != null) {
				if(!isDataDuplicateSinhVien(sinhVien, sv)) {
					Wasehouse.updateData(Integer.parseInt(sv.id),"sinhvien");
					Wasehouse.insertDataSinhVien(sinhVien);
				}
				continue;
			}else {
				Wasehouse.insertDataSinhVien(sinhVien);
			}
		}
		
	}
	
	//convert data from staging to wasehouse(lop hoc)
		public static void convertDataLopHocFromStagingToWasehouse(String idLogs) {
			ArrayList<LopHoc> stagings = Staging.getAllLopHoc();
			LopHoc lh;
			for (LopHoc lopHoc : stagings) {
				lh = Wasehouse.getOneLopHoc(lopHoc.maLop);
				if(lh != null) {
					if(!isDataDuplicateLopHoc(lopHoc, lh)) {
						Wasehouse.updateData(Integer.parseInt(lh.id),"lophoc");
						Wasehouse.insertDataLopHoc(lopHoc);
					}
					continue;
				}else {
					Wasehouse.insertDataLopHoc(lopHoc);
				}
			}
			
		}
		
		//convert data from staging to wasehouse(mon hoc)
				public static void convertDataMonHocFromStagingToWasehouse(String idLogs) {
					ArrayList<MonHoc> stagings = Staging.getAllMonHoc();
					MonHoc mh;
					for (MonHoc monHoc : stagings) {
						mh = Wasehouse.getOneMonHoc(monHoc.maMH);
						if(mh != null) {
							if(!isDataDuplicateMonHoc(monHoc, mh)) {
								Wasehouse.updateData(Integer.parseInt(mh.id),"monhoc");
								Wasehouse.insertDataMonHoc(monHoc);
							}
							continue;
						}else {
							Wasehouse.insertDataMonHoc(monHoc);
						}
					}
					
				}
	
	// kiem tra du lieu trung lap sinh vien
	public static boolean isDataDuplicateSinhVien(SinhVien s, SinhVien w) {
			if(s.lastName.equals(w.lastName) && s.firstName.equals(w.firstName) && s.dayBorn.equals(w.dayBorn) && s.classId.equals(w.classId)
					&& s.className.equals(w.className) && s.phoneNumber.equals(w.phoneNumber) && s.email.equals(w.email) && s.address.equals(w.address) && s.note.equals(w.note)) {
				return true;
		}
		return false;
	}
	
	// kiem tra du lieu trung lap lop hoc
		public static boolean isDataDuplicateLopHoc(LopHoc s, LopHoc w) {
				if(s.maLop.equals(w.maLop) && s.maMon.equals(w.maMon) && s.namHoc.equals(w.namHoc)) {
					return true;
			}
			return false;
		}
		
		// kiem tra du lieu trung lap mon hoc
				public static boolean isDataDuplicateMonHoc(MonHoc s, MonHoc w) {
						if(s.tenMH.equals(w.tenMH) && s.tinChi.equals(w.tinChi) && s.quanLy.equals(w.quanLy) && s.suDung.equals(w.suDung) && s.ghiChu.equals(w.ghiChu)) {
							return true;
					}
					return false;
				}
	
	public static void main(String[] args) {
		convertDataSinhVienFromStagingToWasehouse("2");
		convertDataLopHocFromStagingToWasehouse("2");
		convertDataMonHocFromStagingToWasehouse("2");
//		SinhVien st = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		SinhVien st1 = new SinhVien("1", "mssv", "lastName", "firstName", "dayBorn", "classId", "className", "phoneNumber", "email", "address", "note");
//		System.out.println(dataDuplicate(st, st1));
	}
	

}
