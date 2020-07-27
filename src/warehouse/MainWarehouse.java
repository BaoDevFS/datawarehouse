package warehouse;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;

public class MainWarehouse {
	String tableNameInWarehouseDb, tableNameInStagingDb, field_define_transform, listField, list_colum_datatype;
	int number_colum;
	GetDataFromDB get;
	int countRow = 0;
	public MainWarehouse() {
		get = new GetDataFromDB();
	}

	public boolean checkDuplicate(ResultSet rsData, ResultSet rsWareHouse) {
		try {

			int count = 0;
			for (int i = 1; i <= number_colum; i++) {
				try {
//					System.out.println(rsData.getString(i));
//					System.out.println(rsWareHouse.getObject(i + 1));
					if (!rsData.getString(i).equals(rsWareHouse.getObject(i + 1).toString())) {
						count++;
					}
				} catch (NullPointerException e) {
					e.printStackTrace();
					continue;
				}
			}
			if (count > 2) {
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}

	public void tranferStagingToWarehouse(int idLog, int config) {
		countRow=0;
		ResultSet rsdata = get.getDataInConfig(config);
		try {
			// laays các thuộc tín cần thiết từ config
			rsdata.next();
			tableNameInStagingDb = rsdata.getString("table_name_staging");
			tableNameInWarehouseDb = rsdata.getString("table_name_warehouse");
			field_define_transform = rsdata.getString("field_define_transform");
			number_colum = rsdata.getInt("number_column");
			listField = rsdata.getString("list_field_name");
			list_colum_datatype = rsdata.getString("list_colum_datatype");
			rsdata.close();
			// Kiểm tra table warehouse đã tồn tại hay chưa
			if (!get.checkTableExits(tableNameInWarehouseDb)) {
				// chưua tồn tại thì tạo
				if (!get.createTableWarehouse(tableNameInWarehouseDb, listField, number_colum, list_colum_datatype)) {
					return;
				}
			}
			get.doSpecialTaskInLog(idLog);
			// get data từ satginglen
			rsdata = get.getDataStagingFromDb(tableNameInStagingDb);
			// duyệt từng dòng
			while (rsdata.next()) {
				// kiểm tra số lượng cột trong hàng
				// không đủ thì bỏ qua
//				if (!check_Colum_In_Row(rsdata)) {
//					continue;
//				}
				// get dữ liệu từ bảng warehouse
				ResultSet rsWarehouse = get.getDataFromWarehouse(tableNameInWarehouseDb, field_define_transform, "=",
						rsdata.getString(field_define_transform));
				// nếu có dữ liệu
				PreparedStatement pre = get.intsertDataToWarehouse(tableNameInWarehouseDb, listField, number_colum);
				if (rsWarehouse.next()) {
					if (checkDuplicate(rsdata, rsWarehouse)) {
						continue;
					} else {
//						System.out.println("sadasdasd");
//						// cap expired la ngay hien tai 
						get.updateExpiredInWareHouse(tableNameInWarehouseDb, rsWarehouse.getInt(1),"now()");
						insertRowToWarehouse(pre, rsdata);
					}
				} else {
					// insert duw lieu vao warehouse
					insertRowToWarehouse(pre, rsdata);
				}
			}
			get.updateStatus("OK WH", idLog, countRow);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private void insertRowToWarehouse(PreparedStatement pre, ResultSet rsdata) {
		String[] artoken = list_colum_datatype.split("\\|");
		// set data theo kieeur dữ liệu vào
		int i = 1;
		try {
			String token;
			for (; i <= number_colum; i++) {
				token = artoken[i - 1];
				// kiểm tra kiểu dữ liệu của field
				if (token.startsWith("VARCHAR")) {
					pre.setString(i, rsdata.getString(i));
				} else if (token.startsWith("INT")) {
					if (!token.contains("#")) {
						try {
							pre.setInt(i, Integer.parseInt(rsdata.getString(i)));
						} catch (NumberFormatException e) {
							pre.setInt(i, 0);
							continue;
						}
					} else {
						String[] tk = token.split("#");
//						System.out.println(tk[3]);
//						System.out.println(tk[2]);
//						System.out.println(tk[1]);
//						System.out.println(tk[0]);
						if (tk[3].equals("VARCHAR")) {
							int id = get.getIdFormTableInWarehouse(tk[1], tk[2], rsdata.getString(i));
							pre.setInt(i, id);
						} else if (tk[3].equals("INT")) {
							int id = get.getIdFormTableInWarehouse(tk[1], tk[2], rsdata.getString(i));
							pre.setInt(i, id);
						} else if (tk[3].equals("DATE")) {
							Date date = tranferDate(1, rsdata.getString(i));
							try {
								if (date == null) {
									pre.setInt(i, Integer.parseInt(rsdata.getString(i)));
									continue;
								}
							} catch (NumberFormatException e) {
								pre.setInt(i, 0);
							}
							int id = get.getIdFormTableInWarehouse(tk[1], tk[2],
									date.toInstant().atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toLocalDate().toString());
							pre.setInt(i, id);
						}
					}

				} else if (token.startsWith("DATE")) {
					Date date = tranferDate(1, rsdata.getString(i));
					if (date == null) {
						pre.setDate(i, new java.sql.Date(0000, 00, 00));
						continue;
					}
					pre.setObject(i, date.toInstant().atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toLocalDate());
				}
				// cos the them cac du lieu khac
			}
			pre.setDate(i, new java.sql.Date(9999 - 1900, 11, 31));
			pre.executeUpdate();
			countRow++;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Date tranferDate(int mode, String data) {
		switch (mode) {
		case 1:
			try {
				Date date1 = new SimpleDateFormat("dd/MM/yyyy").parse(data);
				return date1;
			} catch (ParseException e) {
				return tranferDate(2, data);
			}
		case 2:
			try {
				Date date1 = new SimpleDateFormat("yyyy/MM/dd").parse(data);
				return date1;
			} catch (ParseException e) {
				return tranferDate(3, data);
			}
		case 3:
			try {
				Date date1 = new SimpleDateFormat("MM/dd/yyyy").parse(data);
				return date1;
			} catch (ParseException e) {
				return tranferDate(4, data);
			}
		case 4:
			try {
				Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(data);
				return date1;
			} catch (ParseException e) {
				return tranferDate(5, data);
			}
		case 5:
			try {
				Date date1 = new SimpleDateFormat("dd-MM-yyyy").parse(data);
				return date1;
			} catch (ParseException e) {
				return tranferDate(6, data);
			}
		case 6:
			try {
				Date date1 = new SimpleDateFormat("MM-dd-yyyy").parse(data);
				return date1;
			} catch (ParseException e) {
				return null;
			}

		default:
			return null;
		}
	}

	private boolean check_Colum_In_Row(ResultSet rs) {
		try {
			String tmp;
			for (int i = 1; i < number_colum; i++) {
				tmp = rs.getString(i);
				if (tmp == null || tmp.equals(""))
					return false;
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void main(String[] args) {
		MainWarehouse main = new MainWarehouse();
//		Date date = main.tranferDate(1, "30-12-2019");
//		System.out.println(date.toInstant().atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toLocalDate().toString());

//		main.tranferStagingToWarehouse(89, 1);
		for (int i = 14; i < 37; i++) {
			main.tranferStagingToWarehouse(i, 3);
		}
	}
}
