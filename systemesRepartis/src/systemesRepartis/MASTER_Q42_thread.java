package systemesRepartis;

import java.io.IOException;

public class MASTER_Q42_thread {
    public static void main(String[] args) throws IOException {
    	
    	ProcessBuilder pb1 = new ProcessBuilder("ls", "-al", "/tmp");
    	
    	//ProcessBuilder pb1 = new ProcessBuilder("java", "-jar", "/home/bud/Downloads/tmp/inf727_system_repartis/slave.jar");
    	THREAD_STD std_thread = new THREAD_STD(pb1);
    	std_thread.start();
    	
    	THREAD_ERROR error_thread = new THREAD_ERROR(pb1);
    	error_thread.start();
    }
}
