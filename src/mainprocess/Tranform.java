package mainprocess;

import part2.TranfertoStaging;

public class Tranform {
	public static void main(String[] args) {
		TranfertoStaging tranfertoStaging = new TranfertoStaging();
		tranfertoStaging.startTask(Integer.parseInt(args[0]));
	}
}
