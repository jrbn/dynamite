package nl.vu.cs.dynamite.compression;

import nl.vu.cs.ajira.buckets.TupleComparator;

public class URLComparator extends TupleComparator {

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// Here I know that the first field is a String.

		int ls1 = (b1[s1 + 5] << 8) + b1[s1 + 6];
		int ls2 = (b2[s2 + 5] << 8) + b2[s2 + 6];

		return compareBytes(b1, s1 + 7, ls1, b2, s2 + 7, ls2);
	}

}
