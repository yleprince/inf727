package systemesRepartis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class THREAD_STD extends Thread {
	private ProcessBuilder m_pb;

	public THREAD_STD(ProcessBuilder pb) {
		this.m_pb = pb;
	}

	public void run() {
		Process pro;
		try {
			pro = this.m_pb.start();

			BufferedReader std_reader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
			StringBuilder builder = new StringBuilder();
			String std_line = null;

			while ((std_line = std_reader.readLine()) != null) {
				builder.append(std_line);
				builder.append(System.getProperty("line.separator"));
			}

			String std_result = builder.toString();
			System.out.println("STD Reader : " + std_result);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
