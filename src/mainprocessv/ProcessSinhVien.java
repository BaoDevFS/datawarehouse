package mainprocessv;

import getdata.CollectData;
import part2.TranfertoStaging;

public class ProcessSinhVien {

	public void runConfig(int id) {
		CollectData collectData = new CollectData();
		TranfertoStaging tranfertoStaging = new TranfertoStaging();
		Thread th = new Thread(new Runnable() {

			@Override
			public void run() {
				collectData.startTask(id);
			}
		});
		th.start();
		Thread th2 = new Thread(new Runnable() {

			@Override
			public void run() {
				tranfertoStaging.startTask(id);
			}
		});
		th2.start();

	}

	public static void main(String[] args) {
		ProcessSinhVien pr = new ProcessSinhVien();
		pr.runConfig(Integer.parseInt(args[0]));
	}
}
