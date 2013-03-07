package nl.vu.cs.querypie.reasoner.support.sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Tuples {

	private Map<Integer, Collection<Long>> indexes = new HashMap<Integer, Collection<Long>>();
	private Map<Integer, Multimap<Long, long[]>> maps = new HashMap<Integer, Multimap<Long, long[]>>();

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

	public Collection<long[]> get(int pos, long value) {
		if (maps.containsKey(pos)) {
			Multimap<Long, long[]> map = maps.get(pos);
			return map.get(value);
		}

		synchronized (this) {
			Multimap<Long, long[]> map = HashMultimap.create();
			// Populate the map
			for (int i = 0; i < values.length; i += lengthTuple) {
				long key = values[i + pos];
				long[] row = Arrays.copyOfRange(values, i, i + lengthTuple);
				map.put(key, row);
			}
			maps.put(pos, map);
			return map.get(value);
		}
	}
}
