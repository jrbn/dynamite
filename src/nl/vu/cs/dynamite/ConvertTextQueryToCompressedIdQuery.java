package nl.vu.cs.dynamite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;

public class ConvertTextQueryToCompressedIdQuery {
	
	private static HashMap<String, Integer> dictionary = new HashMap<String, Integer>();
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Usage: ConvertTextQueryToCompressedIdQuery [query File] [dictionary file] [output file]");
			return;
		}
		
		readDictionary(args[1]);
		readQueriesAndConvert(args[0], args[2]);
	}

	private static void readDictionary(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				if (line.startsWith("<")) {
					line = line.substring(1, line.length()-4);
					String[] lineSplit = line.split(" ");
					String idComponent = lineSplit[0].substring(0, lineSplit[0].length()-4);
					int id = Integer.parseInt(idComponent);
					dictionary.put(lineSplit[1], id);
				}
				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void readQueriesAndConvert(String input, String output) {
		try {
			PrintWriter pw = new PrintWriter(new File(output));
			BufferedReader br = new BufferedReader(new FileReader(input));
			String line = br.readLine();
			while (line != null) {
				String[] lineSplit = line.split(" ");
				
				for (int i = 0; i < lineSplit.length; i++) {
					if (!lineSplit[i].equals("?")) {
						pw.write(dictionary.get(lineSplit[i]) + " ");
					} else {
						pw.write("? ");
					}
				}
				pw.println();
				line = br.readLine();
			}
			br.close();
			pw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
