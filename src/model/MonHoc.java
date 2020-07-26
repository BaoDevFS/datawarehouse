package model;

public class MonHoc {
	public String id, stt, maMH, tenMH, tinChi, quanLy, suDung, ghiChu, expired;

	public MonHoc(String id, String stt, String maMH, String tenMH, String tinChi, String quanLy, String suDung, String ghiChu, String expired) {
		super();
		this.id = id;
		this.stt = stt;
		this.maMH = maMH;
		this.tenMH = tenMH;
		this.tinChi = tinChi;
		this.quanLy = quanLy;
		this.suDung = suDung;
		this.ghiChu = ghiChu;
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

	public String getMaMH() {
		return maMH;
	}

	public void setMaMH(String maMH) {
		this.maMH = maMH;
	}

	public String getTenMH() {
		return tenMH;
	}

	public void setTenMH(String tenMH) {
		this.tenMH = tenMH;
	}

	public String getTinChi() {
		return tinChi;
	}

	public void setTinChi(String tinChi) {
		this.tinChi = tinChi;
	}

	public String getQuanLy() {
		return quanLy;
	}

	public void setQuanLy(String quanLy) {
		this.quanLy = quanLy;
	}

	public String getSuDung() {
		return suDung;
	}

	public void setSuDung(String suDung) {
		this.suDung = suDung;
	}

	public String getGhiChu() {
		return ghiChu;
	}

	public void setGhiChu(String ghiChu) {
		this.ghiChu = ghiChu;
	}
	
	public String getExpired() {
		return expired;
	}
	
	public void setExpired(String expired) {
		this.expired = expired;
	}
	
}
