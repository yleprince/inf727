package systemesRepartis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class THREAD_ERROR extends Thread {
	private ProcessBuilder m_pb;

	public THREAD_ERROR(ProcessBuilder pb) {
		this.m_pb = pb;
	}

	public void run() {
		Process pro;
		try {
			pro = this.m_pb.start();

			BufferedReader error_reader = new BufferedReader(new InputStreamReader(pro.getErrorStream()));
			StringBuilder builder = new StringBuilder();
			String error_line = null;

			while ((error_line = error_reader.readLine()) != null) {
				builder.append(error_line);
				builder.append(System.getProperty("line.separator"));
			}

			String error_result = builder.toString();
			System.out.println("Error Reader : " + error_result);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}