package model;

public class SinhVien {
	public String id, stt, mssv, firstName, lastName, dayBorn, classId, className, phoneNumber, email, address, note, expired;

	public SinhVien(String id,String stt, String mssv, String lastName,String firstName, String dayBorn, String classId,
			String className, String phoneNumber, String email, String address, String note, String expired) {
		super();
		this.id = id;
		this.stt = stt;
		this.mssv = mssv;
		this.lastName = lastName;
		this.firstName = firstName;
		this.dayBorn = dayBorn;
		this.classId = classId;
		this.className = className;
		this.phoneNumber = phoneNumber;
		this.email = email;
		this.address = address;
		this.note = note;
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


	public String getMssv() {
		return mssv;
	}


	public void setMssv(String mssv) {
		this.mssv = mssv;
	}


	public String getFirstName() {
		return firstName;
	}


	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}


	public String getLastName() {
		return lastName;
	}


	public void setLastName(String lastName) {
		this.lastName = lastName;
	}


	public String getDayBorn() {
		return dayBorn;
	}


	public void setDayBorn(String dayBorn) {
		this.dayBorn = dayBorn;
	}


	public String getClassId() {
		return classId;
	}


	public void setClassId(String classId) {
		this.classId = classId;
	}


	public String getClassName() {
		return className;
	}


	public void setClassName(String className) {
		this.className = className;
	}


	public String getPhoneNumber() {
		return phoneNumber;
	}


	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}


	public String getEmail() {
		return email;
	}


	public void setEmail(String email) {
		this.email = email;
	}


	public String getAddress() {
		return address;
	}


	public void setAddress(String address) {
		this.address = address;
	}


	public String getNote() {
		return note;
	}


	public void setNote(String note) {
		this.note = note;
	}
	
	public String getExpired() {
		return expired;
	}
	public void setExpired(String expired) {
		this.expired = expired;
	}


	@Override
	public String toString() {
		return "id=" + id + "stt=" + stt + ", mssv=" + mssv + ", lastName=" + lastName  + ", firstName=" + firstName
				+ ", dayBorn=" + dayBorn + ", classId=" + classId + ", className=" + className + ", phoneNumber="
				+ phoneNumber + ", email=" + email + ", address=" + address + ", note=" + note + ", expired=" + expired;
	}
	
	

}
