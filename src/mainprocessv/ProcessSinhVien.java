package mainprocessv;

import getdata.CollectData;

public class ProcessSinhVien {
	public ProcessSinhVien() {

	}

	public void runConfig(int id) {
		CollectData collectData = new CollectData();
//		TranfertoStaging tranfertoStaging = new TranfertoStaging();
		collectData.startTask(id);
	}

	public static void main(String[] args) {
		ProcessSinhVien pr = new ProcessSinhVien();
		pr.runConfig(Integer.parseInt(args[0]));
	}
}