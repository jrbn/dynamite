package nl.vu.cs.dynamite;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;

public class QueryIndexedTriples {

	static Vector<Vector<Integer>> triples = new Vector<Vector<Integer>>();
	static final int KEY_NOT_FOUND = -1;

	static void readTriples(String file) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				String[] lineSplit = line.split(" ");
				Vector<Integer> triple = new Vector<Integer>();
				triple.add(Integer.parseInt(lineSplit[1]));
				triple.add(Integer.parseInt(lineSplit[2]));
				triple.add(Integer.parseInt(lineSplit[3]));
				triples.add(triple);
				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static void printTriple(int index) {
		
		System.out.println("Triple "
				+ triples.get(index).get(0) + " " 
				+ triples.get(index).get(1) + " "
				+ triples.get(index).get(2));
	}
	
	public static void main(String[] args) {
		readTriples(args[0]);
		
		int key0 = 27321;
		int right0 = getRightPosition(0, triples.size(), key0, 0);
		int left0 = getLeftPosition(0, right0, key0, 0);
		
		if (triples.get(right0).get(0) == key0 && triples.get(left0).get(0) == key0) {
			if (left0 - 1 >= 0 && triples.get(left0-1).get(0) == key0) {
				left0--;
			}
			printTriple(left0);
			printTriple(right0);

			int key1 =  10688; 
			int right1 = getRightPosition(left0, right0, key1, 1);
			int left1 = getLeftPosition(left0, right0, key1, 1);
			
			if (triples.get(right0).get(0) == key0 && triples.get(left0).get(0) == key0) {
				if (left1 - 1 >= 0 && triples.get(left1-1).get(1) == key1) {
					left1--;
				}
				printTriple(left1);
				printTriple(right1);
				
				double distance = computeAvgDistance(left1, right1);
				System.out.println("Distance " + distance);
			}
		} else {
			System.out.println("Element " + key0 + " not found");
		}
	}
	
	static double computeAvgDistance(int l, int r) {
		int sum = 0;
		for (int i = l+1; i <= r; i++) {
			sum += triples.get(i).get(2) - triples.get(i-1).get(2);
		}
		return sum/(double)(r-l+1);
	}

	static int getRightPosition(int l, int r, int key, int pos) {
		int m;
		while ( r - l > 1 ) {
			m = l + (r - l)/2;
			if (triples.get(m).get(pos) <= key) {
				l = m;
			} else {
				r = m;
			}
		}
		return l;
	}

	static int getLeftPosition( int l, int r, int key, int pos) {
		int m;
		while ( r - l > 1 ) {
			m = l + (r - l)/2;
			if (triples.get(m).get(pos) >= key) {
				r = m;
			} else {
				l = m;
			}
		}
		return r;
	}
}
