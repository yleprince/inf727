package systemesRepartis;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Scanner;

public class MASTER_Q42 {
	public static void main(String[] args) throws Exception {
		Process p = Runtime.getRuntime().exec("ls /jeslmkjs");
		inheritIO(p.getInputStream(), System.out);
		inheritIO(p.getErrorStream(), System.err);

	}

	private static void inheritIO(final InputStream src, final PrintStream dest) {
		new Thread(new Runnable() {
			private Scanner sc;

			public void run() {
				sc = new Scanner(src);
				while (sc.hasNextLine()) {
					dest.println(sc.nextLine());
				}
			}
		}).start();
	}
}
