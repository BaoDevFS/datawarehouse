package warehouse;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.StringTokenizer;

public class MainWarehouse {
	String tableNameInWarehouseDb, tableNameInStagingDb, field_define_transform, listField, list_colum_datatype;
	int number_colum;
	GetDataFromDB get;

	public MainWarehouse() {
		get = new GetDataFromDB();
	}

	public boolean checkDuplicate(ResultSet rsData, ResultSet rsWareHouse) {
		try {

			int count = 0;
			for (int i = 1; i <= number_colum; i++) {
				System.out.println(rsData.getString(i));
				System.out.println(rsWareHouse.getObject(i + 1));
				if (!rsData.getString(i).equals(rsWareHouse.getObject(i + 1))) {
					count++;
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
				get.createTableWarehouse(tableNameInWarehouseDb, listField, number_colum, list_colum_datatype);
			}
			// get data từ satginglen
			rsdata = get.getDataStagingFromDb(tableNameInStagingDb);
			// duyệt từng dòng
			while (rsdata.next()) {
				// kiểm tra số lượng cột trong hàng
				// không đủ thì bỏ qua
				if (!check_Colum_In_Row(rsdata)) {
					continue;
				}
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
						get.updateExpiredInWareHouse(tableNameInWarehouseDb, rsWarehouse.getInt(1));
						insertRowToWarehouse(pre, rsdata);
					}
				} else {
					// insert duw lieu vao warehouse
					insertRowToWarehouse(pre, rsdata);
				}
			}
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
			for (; i <= number_colum; i++) {
				if (artoken[i - 1].contains("VARCHAR")) {
					pre.setString(i, rsdata.getString(i));
				} else if (artoken[i - 1].contains("INT")) {
					try {
						pre.setInt(i, Integer.parseInt(rsdata.getString(i)));
					} catch (NumberFormatException e) {
						pre.setInt(i, 0);
						continue;
					}
				} else if (artoken[i - 1].contains("DATE")) {
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
				Date date1 = new SimpleDateFormat("dd-MM-yyyy").parse(data);
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
			for (int i = 1; i < 11; i++) {
				tmp = rs.getString(i);
				if (tmp.equals("") || tmp == null)
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
		main.tranferStagingToWarehouse(89, 1);
	}
}
