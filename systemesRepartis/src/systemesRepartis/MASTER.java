package systemesRepartis;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MASTER {
    public static void main(String[] args) throws IOException {
    	
    	//ProcessBuilder pb1 = new ProcessBuilder("ls", "-al", "/tmp");
    	
    	ProcessBuilder pb1 = new ProcessBuilder("java", "-jar", "/home/bud/Downloads/tmp/inf727_system_repartis/slave.jar");
    	Process pro = pb1.start();
    	BufferedReader reader = new BufferedReader(new InputStreamReader(pro.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line = null;

        while ( (line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(System.getProperty("line.separator"));
        }

        String result = builder.toString();
        System.out.println("Reader : " + result);
    }
}
