package systemesRepartis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MASTER_PART_9 {
	public static void main(String[] args) throws IOException, InterruptedException {

		// long timeout = 2; // Test 1

		// long timeout = 15; // Test 2
		// ProcessBuilder pb45 = new ProcessBuilder("java", "-jar",
		// "/home/bud/Downloads/tmp/slave.jar");

		// ProcessBuilder pb45 = new ProcessBuilder("java", "-jar",
		// "/home/bud/Downloads/tmp/slave_Q45_test3.jar"); // Test3
		// getResponse(pb45, timeout);

		System.out.println("Searching 3 computers");
		ArrayList<String> computers = findComputers(3);
		
		Map<String, String> masterMap = new HashMap<String, String>();
		
		System.out.println(computers);
		question47(computers);
		masterMap = question48(computers);
		question50(masterMap);
		
	}

	public static void question50(Map<String, String> masterMap) {
		System.out.println("Question 50 -- start\n");
		for (String name: masterMap.keySet()){

            String key =name.toString();
            String value = masterMap.get(name).toString();  
            System.out.println("\t" + key + " " + value);  


}
		System.out.println("\nQuestion 50 -- end");

	}
	
	public static HashMap<String, String> question48(ArrayList<String> computers) throws IOException, InterruptedException {
		System.out.println("Question 48 -- start\n");
		System.out.println("Deploying splits on distant computers");

		// 0. Init
		String content0 = "Deer Beer River";
		String content1 = "Car Car River";
		String content2 = "Deer Car Beer";
		ArrayList<String> contents = new ArrayList<String>();
		contents.add(content0);
		contents.add(content1);
		contents.add(content2);

		Map<String, String> masterMap = new HashMap<String, String>();
		
		// 1. create files
		String file;
		String inputPath = "/home/bud/Downloads/tmp/";
		String outputPath = "/tmp/yleprince/split/";

		for (int i = 0; i < contents.size(); i++) {
			file = "split" + i + ".txt";

			PrintWriter writer = new PrintWriter(inputPath + file, "UTF-8");
			writer.println(contents.get(i));
			writer.close();
			
			String pc = computers.get(i);
			deployOnComputer(pc, inputPath + file, outputPath);
			masterMap.put(file, pc);
		}

		System.out.println("\nQuestion 48 -- end");
		return (HashMap<String, String>) masterMap;
	}

	public static void question47(ArrayList<String> computers) throws IOException, InterruptedException {
		System.out.println("Question 47 -- start\n");
		System.out.println("Deploying slave.jar on computers");
				
		String inputPath = "/tmp/slave_map.jar";
		String outputPath = "/tmp/yleprince/jar/";
		for(String pc : computers) {
			deployOnComputer(pc, inputPath, outputPath);
		}
		System.out.println("\nQuestion 47 -- end");
	}

	public static String mkDir(String pc, String outputPath) throws IOException, InterruptedException {
		String response_mkdir;
		ProcessBuilder pb_mkdir = new ProcessBuilder("ssh", "-o", "StrictHostKeyChecking=no", "yleprince@" + pc,
				"mkdir", "-p", outputPath);
		response_mkdir = getResponse(pb_mkdir, 5);
		return response_mkdir;
	}

	public static String scp(String pc, String inputPath, String outputPath) throws IOException, InterruptedException {
		String response_scp;
		String user = "yleprince";
		ProcessBuilder pb_scp = new ProcessBuilder("scp", inputPath, user + "@" + pc + ":" + outputPath);
		response_scp = getResponse(pb_scp, 5);
		return response_scp;
	}

	public static void deployOnComputer(String pc, String inputPath, String outputPath) throws IOException, InterruptedException {
		String response_mkdir;
		String response_scp;
		
		response_mkdir = mkDir(pc, outputPath);
		System.out.println("\tDeployed on: " + pc + " at " + outputPath);

		response_scp = scp(pc, inputPath, outputPath);
		System.out.println("\tCopied on: " + pc + response_scp);
	}
	
	public static void deploy(String computerListFilename, String inputPath, String outputPath)
			throws IOException, InterruptedException {

		System.out.println("Enter Deploy");
		ArrayList<String> computersUsables = deployable(computerListFilename);
		System.out.println("computersUsables" + computersUsables);

		System.out.println("Loop Deploy");
		for (String pc : computersUsables) {

			deployOnComputer(pc, inputPath, outputPath);
			System.out.println();
		}

		System.out.print("End deploy");
	}

	public static ArrayList<String> deployable(String filename) throws IOException, InterruptedException {

		ArrayList<String> computersNames = readFileLineByLine(filename);
		ArrayList<String> computersUsables = new ArrayList<String>();

		for (String pc : computersNames) {
			if (isComputerUsable(pc)) {
				computersUsables.add(pc);
			}
		}
		return computersUsables;
	}

	public static boolean isComputerUsable(String pc) throws IOException, InterruptedException {
		String response;
		ProcessBuilder pb = new ProcessBuilder("ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=1", "yleprince@" + pc, "hostname");
		response = getResponse(pb, 5);
		if (response != null) {
			return true;
		} else {
			return false;
		}
	}

	public static ArrayList<String> findComputers(int nComputers) throws IOException, InterruptedException {
		ArrayList<String> computersAvailable = new ArrayList<String>();
		ArrayList<String> computers = new ArrayList<String>();

		for (int i = 10; i < 40; ++i) {
			computers.add("c130" + "-" + i);
			computers.add("c133" + "-" + i);
		}

		int iterator= 0;
		while (computersAvailable.size()<nComputers) {
			if (isComputerUsable(computers.get(iterator))) {
				computersAvailable.add(computers.get(iterator));
			}
			iterator += 1;
		}
		return computersAvailable;
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

			b.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
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
			// System.out.println("Response std : " + response);
			return response;
		} else if (response.length() == 0 && responseErr != null) {
			System.err.println("Error : " + responseErr);
		} else {
			System.out.println("Response std: " + response);
			System.err.println("Error : " + responseErr);
		}
		p.destroy();
		t.interrupt();
		te.interrupt();
		return null;
	}

}