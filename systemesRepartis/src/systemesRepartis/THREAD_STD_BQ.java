package systemesRepartis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class THREAD_STD_BQ extends Thread {
	private Process m_p;
	private BlockingQueue<String> m_bq;

	public THREAD_STD_BQ(Process p, BlockingQueue<String> blocking_queue) {
		this.m_p = p;
		this.m_bq = blocking_queue;
	}

	@Override
	public void run() {
		BufferedReader std_reader = new BufferedReader(new InputStreamReader(this.m_p.getInputStream()));
		StringBuilder builder = new StringBuilder();
		String std_line = null;

		try {
			while ((std_line = std_reader.readLine()) != null) {
				builder.append(std_line);
				builder.append(System.getProperty("line.separator"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String std_result = builder.toString();
		//System.out.println("STD Reader : " + std_result);
		
		
        try {
            this.m_bq.put(std_result);
        } catch (InterruptedException ex) {
            Logger.getLogger(THREAD_STD_BQ.class.getName()).log(Level.SEVERE, null, ex);
        }
	}
}