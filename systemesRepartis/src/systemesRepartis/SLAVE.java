package systemesRepartis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SLAVE {
	public static void main(String[] args) throws InterruptedException, IOException {

		// /tmp/yleprince/splits/S1.txt // java​ ​ -jar​ ​
		// java -jar ./jar/slave_map.jar 0 ./split/split2.txt
		analyzeArgs(args); // Sx -> UMx & UMx -> SMx
	}

	public static void analyzeArgs(String[] args) throws IOException, InterruptedException {
		// System.out.println(args);

		String mode = args[0];
		String work_dir_path = "/tmp/yleprince/";

		if (mode.equals("0")) {
			String filename = args[1];
			String filenameNumber = systemesRepartis.MASTER_PART_10.findFileNumber(filename);
			//System.out.println("Launching the mode 0: from split creates an um");
			split_to_um(work_dir_path, filenameNumber);
		} else if (mode.equals("1")) {
			String key = args[1].toLowerCase();
			//System.out.println("Launching the mode 1: from um creates an sm");
			String SMfilenamePath = reduce_part1(work_dir_path, key, args);
			String RMfilenamePath = reduce_part2(work_dir_path, key, SMfilenamePath);
			System.out.println(RMfilenamePath);
		}

	}

	public static String reduce_part2(String work_dir_path, String key, String SMfilenamePath) throws IOException, InterruptedException {
		mkDirLocal(work_dir_path + "reduces/");
		String rmNumber = systemesRepartis.MASTER_PART_10.findFileNumber(SMfilenamePath);
		String RMfilenamePath = work_dir_path + "reduces/RM" + rmNumber + ".txt";		
		
		int nbOccurences = getNbLines(SMfilenamePath);

		PrintWriter writer = new PrintWriter(RMfilenamePath, "UTF-8");
		writer.println(key + " " + nbOccurences);
		writer.close();
		
		return RMfilenamePath;
	}
	
	public static int getNbLines (String filename) throws IOException {
		File f = new File(filename);
		BufferedReader b = new BufferedReader(
				new FileReader(f));
		int nbLines = 0;
		while ((b.readLine()) != null) {
			nbLines += 1;
		}
		b.close();
		return nbLines;
	}
	
	public static String reduce_part1(String work_dir_path, String key, String[] args) throws IOException, InterruptedException {
		/*
		 * Take a key as input, and extract this key from the UMx files. Exports the SM
		 * file to the output path given.
		 */
		String smNumber = args[2];
		ArrayList<String> umxFiles = new ArrayList<String>();

		for (int i = 3; i < args.length; ++i) {
			umxFiles.add(args[i]);
		}

		// Create output folder
		mkDirLocal(work_dir_path + "map/");
		String SMfilenamePath = work_dir_path + "map/SM" + smNumber + ".txt";

		createSM(SMfilenamePath, umxFiles, key);
		
		return SMfilenamePath;
	}

	public static void createSM(String SMfilenamePath, ArrayList<String> umxFiles, String key) throws IOException {

		ArrayList<String> lines = new ArrayList<String>();
		for (String file : umxFiles) {
			File f = new File(file);
			BufferedReader b = new BufferedReader(new FileReader(f));
			String readLine = "";
			while ((readLine = b.readLine()) != null) {
				if (key.equals(readLine.split("\\s+")[0].toLowerCase())) {
					lines.add(readLine);
				}
			}
			b.close();
		}

		// 3. export the sm file
		PrintWriter writer = new PrintWriter(SMfilenamePath, "UTF-8");
		for (String line : lines) {
			writer.println(line);
		}
		writer.close();
	}

	public static void split_to_um(String work_dir_path, String instanceID) throws IOException, InterruptedException {
		// 1. Get the file content
		ArrayList<String> lines = readFileLineByLine(work_dir_path + "split/split" + instanceID + ".txt");

		if (!lines.isEmpty()) {

			mkDirLocal(work_dir_path + "UM/");
			String outputFilename = work_dir_path + "UM/UM" + instanceID + ".txt";

			// 2. do the map
			ArrayList<String> words = new ArrayList<String>();
			for (String line : lines) {
				for (String word : line.split("\\s+")) { // split on all type of spaces
					words.add(word);
				}
			}

			// 3. print the keys in console
			Set<String> keys = new HashSet<String>(words);
			for (String k : keys) {
				System.out.println(k);
			}
			
			// 4. export the mapped file
			PrintWriter writer = new PrintWriter(outputFilename, "UTF-8");
			for (String word : words) {
				writer.println(word + " 1");
			}
			writer.close();
		} else {
			System.err.println("Split file is empty.");
		}
	}

	public static void mkDirLocal(String path) throws IOException, InterruptedException {
		/** Create a new folder */
		ProcessBuilder pb_mkdir = new ProcessBuilder("mkdir", "-p", path);
		pb_mkdir.start();
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

	public static String findPathFromFilename(String filename) {
		String pattern = "\\/([A-Za-z]+\\d+\\.txt)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(filename);
		String file = null;

		if (m.find()) {
			file = m.group(0);
		} else {
			System.err.println("NO MATCH: error during regex");
		}
		String pathSectionToRemove = "splits/";
		String path = filename.substring(0, filename.length() + 1 - file.length() - pathSectionToRemove.length());
		return path;
	}
}
