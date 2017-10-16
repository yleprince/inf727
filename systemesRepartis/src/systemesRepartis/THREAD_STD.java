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
			pro = m_pb.start();

			BufferedReader std_reader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
			StringBuilder builder = new StringBuilder();
			String line = null;

			while ((line = std_reader.readLine()) != null) {
				builder.append(line);
				builder.append(System.getProperty("line.separator"));
			}

			String result = builder.toString();
			System.out.println("Reader : " + result);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
