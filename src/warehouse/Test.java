package warehouse;

import java.util.Arrays;

public class Test {
	public static void main(String[] args) {
		
		String sql = "Select sql_for_special_task from logs where id =?";
		String a = "Hien|Hau|Hieu|Hien";
		String[] ar = a.split("\\|");
		System.out.println(Arrays.toString(ar));
		System.out.println(ar[0]);
	}

}
