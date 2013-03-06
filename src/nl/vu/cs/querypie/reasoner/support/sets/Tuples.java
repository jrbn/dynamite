package nl.vu.cs.querypie.reasoner.support.sets;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class Tuples {

	private Map<Integer, Collection<Long>> indexes = new HashMap<Integer, Collection<Long>>();

	private int lengthTuple;
	private long[] values;

	public Tuples(int lengthTuple, long[] values) {
		this.lengthTuple = lengthTuple;
		this.values = values;
	}

	public Collection<Long> getSortedSet(int pos) {
		if (indexes.containsKey(pos)) {
			return indexes.get(pos);
		}

		// Calculate the set and returns it
		synchronized (this) {
			TreeSet<Long> set = new TreeSet<Long>();
			for (int i = 0; i < values.length; i += lengthTuple) {
				set.add(values[i + pos]);
			}
			indexes.put(pos, set);
			return set;
		}
	}

	public RowSet get(int pos, long value) {
		// TODO Auto-generated method stub
		return null;
	}
}
