package nl.vu.cs.dynamite.compression;

import nl.vu.cs.ajira.buckets.TupleComparator;

public class TripleIDComparator extends TupleComparator {

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// Here I know that the first field is a long.
		int i = 3;
		int response = 0;
		do {
			response = b1[s1 + i] - b2[s2 + i];
		} while (response == 0 && ++i < 11); // 8+3
		return response;
	}
}
