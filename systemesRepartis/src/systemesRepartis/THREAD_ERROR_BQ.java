package systemesRepartis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class THREAD_ERROR_BQ extends Thread {
	private Process m_p;
	private BlockingQueue<String> m_bq;

	public THREAD_ERROR_BQ(Process p, BlockingQueue<String> blocking_queue) {
		this.m_p = p;
		this.m_bq = blocking_queue;
	}

	@Override
	public void run() {
		BufferedReader err_reader = new BufferedReader(new InputStreamReader(this.m_p.getErrorStream()));
		StringBuilder builder = new StringBuilder();
		String err_line = null;

		try {
			while ((err_line = err_reader.readLine()) != null) {
				builder.append(err_line);
				builder.append(System.getProperty("line.separator"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		String err_result = builder.toString();
		// System.out.println("STD Reader : " + std_result);

		try {
			this.m_bq.put(err_result);
		} catch (InterruptedException ex) {
			Logger.getLogger(THREAD_STD_BQ.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}


