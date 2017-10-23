package systemesRepartis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MASTER_PART_8 {
	public static void main(String[] args) throws IOException, InterruptedException {

		// long timeout = 2; // Test 1

		long timeout = 15; // Test 2
		// ProcessBuilder pb45 = new ProcessBuilder("java", "-jar",
		// "/home/bud/Downloads/tmp/slave.jar");

		ProcessBuilder pb45 = new ProcessBuilder("java", "-jar", "/home/bud/Downloads/tmp/slave_Q45_test3.jar"); // Test3
		// getResponse(pb45, timeout);

		ArrayList<String> computersAddresses = readFileLineByLine("/home/bud/Documents/s1/inf727/computersOn.txt");

		
		
	}

	public static String[] deploy() {

		return null;
	}

	public static ArrayList<String> readFileLineByLine(String filename) {
		/**
		 * Return an ArrayList<String> where all value is a line of the file
		 */

		ArrayList<String> lines = new ArrayList<String>();
		try {

			File f = new File(filename);
			BufferedReader b = new BufferedReader(new FileReader(f));
			String readLine = "";

			while ((readLine = b.readLine()) != null) {
				lines.add(readLine);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getResponse(ProcessBuilder pb, long timeout) throws IOException, InterruptedException {
		Process p = pb.start();

		BlockingQueue<String> blocking_queue = new ArrayBlockingQueue<String>(1024);
		THREAD_STD_BQ t = new THREAD_STD_BQ(p, blocking_queue); // Creation du thread reader std
		t.start();
		String response = blocking_queue.poll(timeout, TimeUnit.SECONDS);

		BlockingQueue<String> blocking_queue_error = new ArrayBlockingQueue<String>(1024);
		THREAD_ERROR_BQ te = new THREAD_ERROR_BQ(p, blocking_queue_error); // Creation du thread reader err
		te.start();
		String responseErr = blocking_queue_error.poll(timeout, TimeUnit.SECONDS);

		if (response == null && responseErr == null) {
			System.err.println("Process killed by timeout");
		} else if (response != null && responseErr.length() == 0) {
			System.out.println("Response : " + response);
			return response;
		} else if (response.length() == 0 && responseErr != null) {
			System.err.println("Error : " + responseErr);
		} else {
			System.out.println("Response : " + response);
			System.err.println("Error : " + responseErr);
		}
		p.destroy();
		t.interrupt();
		te.interrupt();
		return "null";
	}

}