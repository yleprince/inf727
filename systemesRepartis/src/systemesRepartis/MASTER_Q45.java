package systemesRepartis;

import java.io.IOException;

public class MASTER_Q45 {
	public static void main(String[] args) throws IOException {

		// ProcessBuilder pb1 = new ProcessBuilder("ls", "-al", "/tmp");
		ProcessBuilder pb1 = new ProcessBuilder("java", "-jar",
				"/home/bud/Downloads/tmp/inf727_system_repartis/slave.jar");

		// TODO: RELIRE LA QUESTION (PARTIE ASTUCE EN BAS DE Q45)
		// ArrayBlockingQueue
		/*
		 * int timeout = 2000; long startTime = System.currentTimeMillis();
		 * 
		 * THREAD_STD std_thread = new THREAD_STD(pb1); THREAD_ERROR error_thread = new
		 * THREAD_ERROR(pb1);
		 * 
		 * while (System.currentTimeMillis() < startTime + timeout) {
		 * std_thread.start(); error_thread.start(); }
		 * 
		 * std_thread.stop(); error_thread.stop();
		 */
	}
}