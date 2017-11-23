package systemesRepartis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SLAVE {
	public static void main(String[] args) throws InterruptedException, IOException {

		// question44();
		// question45();
		//question49(args); //  /tmp/yleprince/splits/S1.txt // java​ ​ -jar​ ​ slave.jar​ */
		question51(args);
	}

	public static void question51(String[] args) throws IOException, InterruptedException {
		
		question49(args);
		// 1. Get the file content
		ArrayList<String> lines = readFileLineByLine(args[0]);
		HashSet<String> hset = new HashSet<String>();
		
		if (!lines.isEmpty()) {
			// 2. do the map
			for (String line : lines) {
				for (String word : line.split("\\s+")) {
					hset.add(word);
				}
			}
		}
		
		for (String uniqueWord: hset) {
			System.out.println(uniqueWord);
		}

	}
	
	
	public static void mkDirLocal(String path) throws IOException, InterruptedException {
		ProcessBuilder pb_mkdir = new ProcessBuilder("mkdir", "-p", path);
		pb_mkdir.start();
		//System.out.println("\tMkdir: " + path);
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


	public static void question49(String[] args) throws IOException, InterruptedException {
		if (args.length != 1) {
			System.err.println("Erreur: pas ou trop d'arguments pour le slave");
		}

		// 0. Init
		String filename = args[0];
		String filenameNumber = systemesRepartis.MASTER_PART_9.findFileNumber(filename);
		String filenamePath = findPathFromFilename(filename) + "/UM/";

		// 1. Get the file content
		ArrayList<String> lines = readFileLineByLine(filename);

		if (!lines.isEmpty()) {

			mkDirLocal(filenamePath);
			String outputFilename = filenamePath + "UM" + filenameNumber + ".txt";

			// 2. do the map
			ArrayList<String> words = new ArrayList<String>();
			for (String line : lines) {
				for (String word : line.split("\\s+")) {
					words.add(word);
				}
			}

			// 3. export the mapped file
			PrintWriter writer = new PrintWriter(outputFilename, "UTF-8");
			for (String word : words) {
				writer.println(word + " 1");
			}
			writer.close();
		}
	}

	public static void question45() {
		int result = 3 + 5;
		System.err.print("3 + 5 = ");
		System.err.print(result);
		System.err.println("");
	}

	public static void question44() throws InterruptedException {
		Thread.sleep(10000);

		int result = 3 + 5;
		System.out.print("3 + 5 = ");
		System.out.print(result);
		System.out.println("");
	}
}
