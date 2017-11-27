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
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class MASTER_PART_10 {

	private static final String WORKING_DIR = "/tmp/yleprince/";

	public static void main(String[] args) throws IOException, InterruptedException {
		
		System.out.println("Searching 3 computers");
		ArrayList<String> computers = findComputers(3);
		
		System.out.println(computers);
		Map<String, String> masterMap = new HashMap<String, String>();

		cleanDistantDirectories(computers);
		
		question47(computers);	// deploy jar
		masterMap = question48(computers); // deploy splits
		//question50(masterMap);	// launch jar on splits
		masterMap = question52(masterMap);	// launch jar on splits
		question53(masterMap);	// wait until all UMs exist
		question56(masterMap);
	}
	

	public static void question56(Map<String, String> masterMap) {
		
	}
	public static void question53(Map<String, String> masterMap) throws IOException, InterruptedException {
		System.out.println("Question 53 -- start\n");
		
		System.out.println("Waiting until the end of the mapping process.");

		Boolean fileExist = false;
		while (fileExist == false){
			fileExist = true;
			for (String name: masterMap.keySet()){
	            String filePath = name.toString();
	            String pc = masterMap.get(name);
	            ProcessBuilder pb_fileExist = new ProcessBuilder("ssh", "yleprince@" + pc, "test", "-f", filePath, "&&", "echo", "found", "||", "echo", "not", "found");
				String rep_fileExist = getResponse(pb_fileExist, 5);
				if (rep_fileExist == "not found") {
					fileExist = false;
					break;
				}	            
			}
		}
		
		System.out.println("Phase de MAP terminée");
		
		System.out.println("\nQuestion 53 -- end");
	}
	
	public static boolean map(Map<String, String> masterMap_splitsFile_pc) throws IOException, InterruptedException {
		Map<String, ArrayList<String>> keyToUMx = new HashMap<String, ArrayList<String>>();
		String jarPath = WORKING_DIR + "jar/slave_map.jar";
		
		System.out.println("map");
		for (String splitFile : masterMap_splitsFile_pc.keySet()) {
			String pc = masterMap_splitsFile_pc.get(splitFile);
			
			System.out.println("\tLaunching " + jarPath + " with " + splitFile + " at " + pc + " | mode 0.");
			String response_jar = launchJar(pc, jarPath, splitFile);
			
			String umName = "UM"+findFileNumber(splitFile);
			for (String word : response_jar.split("\n")) {
				ArrayList<String> umx = new ArrayList<String>();
				if (keyToUMx.containsKey(word)) {
					umx = keyToUMx.get(word);
				}
				umx.add(umName);
				keyToUMx.put(word, umx);
			}
		}
		System.out.println(keyToUMx);
		
		
		return true;
	}

	public static HashMap<String, String> question52(Map<String, String> masterMap) throws IOException, InterruptedException {
		System.out.println("Question 52 -- start\n");
		
		System.out.println("Launching the jar on the splits");

		Map<String, ArrayList<String>> keyToUMx = new HashMap<String, ArrayList<String>>();
		
		for (String splitFile : masterMap.keySet()) {
			String pc = masterMap.get(splitFile);
			String jarPath = WORKING_DIR + "jar/slave_map.jar";
			System.out.println("\tLaunching " + jarPath + " with " + splitFile + " at " + pc);
			String mode = "0"; //Transform split to um
			String response_jar = launchJarWithOption(pc, jarPath, mode, splitFile);
			
			String umName = "UM"+findFileNumber(splitFile);
			for (String word : response_jar.split("\n")) {
				ArrayList<String> umx = new ArrayList<String>();
				if (keyToUMx.containsKey(word)) {
					umx = keyToUMx.get(word);
				}
				umx.add(umName);
				keyToUMx.put(word, umx);
			}
		}
		
		System.out.println("\nMaster map UM-pc:");
		masterMap = updateMasterMapToUM(masterMap);
		for (String name: masterMap.keySet()){
            String key = name.toString();
            String value = masterMap.get(name);  
            System.out.println("\tFile: " + key + "\t-- pc: " + value);  
		}
		
		System.out.println("\nMaster map word-UMx:");
		for (String word: keyToUMx.keySet()){
            String key = word;
            ArrayList<String> value = keyToUMx.get(word);  
            System.out.println("\tWord: " + key + "\t-- Umx: " + value);  
		}


		System.out.println("\nQuestion 52 -- end");
		
		return (HashMap<String, String>) masterMap;

	}
	
	public static void question50(Map<String, String> masterMap) throws IOException, InterruptedException {
		System.out.println("Question 50 -- start\n");
		System.out.println("Launching the jar on the splits");

		
		for (String splitFile : masterMap.keySet()) {
			String pc = masterMap.get(splitFile);
			String jarPath = WORKING_DIR + "jar/slave_map.jar";
			System.out.println("\tLaunching " + jarPath + " with " + splitFile + " at " + pc);
			String response_jar = launchJar(pc, jarPath, splitFile);
			System.out.println(response_jar);
		}
		
		System.out.println("\nMaster map UM:");
		masterMap = updateMasterMapToUM(masterMap);
		for (String name: masterMap.keySet()){
            String key = name.toString();
            String value = masterMap.get(name);  
            System.out.println("\tFile: " + key + " -- pc: " + value);  
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
		String inputPath = "/tmp/";
		String outputPath = WORKING_DIR + "split/";

		for (int i = 0; i < contents.size(); i++) {
			file = "split" + i + ".txt";

			PrintWriter writer = new PrintWriter(inputPath + file, "UTF-8");
			writer.println(contents.get(i));
			writer.close();
			
			String pc = computers.get(i);
			deployOnComputer(pc, inputPath + file, outputPath);
			
			masterMap.put(outputPath + file, pc);
		}

		System.out.println("\nQuestion 48 -- end");
		return (HashMap<String, String>) masterMap;
	}

	public static void question47(ArrayList<String> computers) throws IOException, InterruptedException {
		System.out.println("Question 47 -- start\n");
		System.out.println("Deploying slave.jar on computers");
				
		String inputPath = "/tmp/slave_map.jar";
		String outputPath = WORKING_DIR + "jar/";
		for(String pc : computers) {
			deployOnComputer(pc, inputPath, outputPath);
		}
		System.out.println("\nQuestion 47 -- end");
	}

	public static void cleanDistantDirectories(ArrayList<String> computers) throws IOException, InterruptedException {
		for (String pc : computers) {
			ProcessBuilder pb_clear = new ProcessBuilder("ssh", "yleprince@" + pc, "rm", "-rf", "/tmp/yleprince");
			getResponse(pb_clear, 5);
			System.out.println("\t" + pc + " has been cleaned.");
		}
		
		System.out.println("\n");
	}
	
	public static String mkDir(String pc, String outputPath) throws IOException, InterruptedException {
		String response_mkdir;
		ProcessBuilder pb_mkdir = new ProcessBuilder("ssh", "yleprince@" + pc, "mkdir", "-p", outputPath);
		response_mkdir = getResponse(pb_mkdir, 5);
		return response_mkdir;
	}
	
	public static String launchJarWithOption(String pc, String jarPath, String mode, String arguments) throws IOException, InterruptedException {
		ProcessBuilder pb_jar = new ProcessBuilder("ssh", "yleprince@" + pc, "java", "-jar", jarPath, mode, arguments);
		String response_jar = getResponse(pb_jar, 5);
		return response_jar;
	}
	
	public static String launchJar(String pc, String jarPath, String arguments) throws IOException, InterruptedException {
		ProcessBuilder pb_jar = new ProcessBuilder("ssh", "yleprince@" + pc, "java", "-jar", jarPath, arguments);
		String response_jar = getResponse(pb_jar, 5);
		return response_jar;
	}

	public static String scp(String pc, String inputPath, String outputPath) throws IOException, InterruptedException {
		String response_scp;
		String user = "yleprince";
		ProcessBuilder pb_scp = new ProcessBuilder("scp", inputPath, user + "@" + pc + ":" + outputPath);
		response_scp = getResponse(pb_scp, 5);
		return response_scp;
	}

	public static void deployOnComputer(String pc, String inputPath, String outputPath) throws IOException, InterruptedException {
						
		String response_mkdir = mkDir(pc, outputPath);
		System.out.println("\tDeployed on: " + pc + " at " + outputPath + response_mkdir);

		String response_scp = scp(pc, inputPath, outputPath);
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

		for (int i = 10; i < 50; ++i) {
			computers.add("c45" + "-" + i);
			computers.add("c126" + "-" + i);
			computers.add("c127" + "-" + i);
			computers.add("c128" + "-" + i);
			computers.add("c129" + "-" + i);
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

	public static String findFileNumber(String filename) {

		String pattern = "(\\d+)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(filename);

		String fileNumber = null;

		if (m.find()) {
			fileNumber = m.group(0);
		} else {
			System.err.println("NO MATCH: need to give a filename containing at least a digit");
		}
		return fileNumber;
	}
	
	public static Map<String, String> updateMasterMapToUM(Map<String, String> masterMapSPLIT){
		Map<String, String> masterMapUM = new HashMap<String, String>();
		for (String file : masterMapSPLIT.keySet()) {
			String fileID = findFileNumber(file);
			String pc = masterMapSPLIT.get(file);
			String newFileName = WORKING_DIR + "UM/UM" + fileID + ".txt";
			masterMapUM.put(newFileName, pc); 
		}
		return masterMapUM;
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