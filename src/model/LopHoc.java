package model;

public class LopHoc {
	public String id, stt, maLop, maMon, namHoc, expired;
 
	public LopHoc(String id, String stt, String maLop, String maMon, String namHoc, String expired) {
		super();
		this.id = id;
		this.stt = stt;
		this.maLop = maLop;
		this.maMon = maMon;
		this.namHoc = namHoc;
		this.expired = expired;
	}
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}

	public String getStt() {
		return stt;
	}

	public void setStt(String stt) {
		this.stt = stt;
	}

	public String getMaLop() {
		return maLop;
	}

	public void setMaLop(String maLop) {
		this.maLop = maLop;
	}

	public String getMaMon() {
		return maMon;
	}

	public void setMaMon(String maMon) {
		this.maMon = maMon;
	}

	public String getNamHoc() {
		return namHoc;
	}

	public void setNamHoc(String namHoc) {
		this.namHoc = namHoc;
	}
	
	public String getExpired() {
		return expired;
	}
	
	public void setExpired(String expired) {
		this.expired = expired;
	}
	@Override
	public String toString() {
		return "LopHoc [id=" + id + ", stt=" + stt + ", maLop=" + maLop + ", maMon=" + maMon + ", namHoc=" + namHoc
				+ ", expired=" + expired + "]";
	}
	

}
