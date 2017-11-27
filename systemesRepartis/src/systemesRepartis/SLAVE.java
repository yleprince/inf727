package systemesRepartis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
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

		if (args.length != 2) {
			System.err.println("Erreur: pas ou trop d'arguments pour le slave");
		}

		String mode = args[0];
		String filename = args[1];
		String filenameNumber = systemesRepartis.MASTER_PART_10.findFileNumber(filename);
		String work_dir_path = "/tmp/yleprince/";

		if (mode.equals("0")) {
			System.out.println("Launching the mode 0: from split creates an um");
			split_to_um(work_dir_path, filenameNumber);
		} else if (true) {
			System.out.println("Launching the mode 1: from um creates an sm");
			//split_to_um(work_dir_path, filenameNumber);
		}

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

			// 3. export the mapped file
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

