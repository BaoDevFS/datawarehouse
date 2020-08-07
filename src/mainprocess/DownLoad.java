package mainprocess;

import getdata.CollectData;

public class DownLoad {
	public static void main(String[] args) {
		CollectData collect = new CollectData();
		collect.startTask(Integer.parseInt(args[0]));
	}
}
