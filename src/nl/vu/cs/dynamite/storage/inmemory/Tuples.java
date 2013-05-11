package nl.vu.cs.dynamite.storage.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class Tuples {

	public static class Row {
		int step;
		Tuple tuple;

		Row(int step, Tuple tuple) {
			this.step = step;
			this.tuple = tuple;
		}

		public int getStep() {
			return step;
		}

		public Tuple getTuple() {
			return tuple;
		}

		public TLong getValue(int pos) {
			return (TLong) tuple.get(pos);
		}

	}

	private final List<Row> rows;
	private final int tuplesLength;

	// Position -> Sorted values
	private final Map<Integer, SortedSet<Long>> indexes = new HashMap<Integer, SortedSet<Long>>();
	// Position -> Value -> Step
	private final Map<Integer, Map<Long, Integer>> indexesWithStep = new HashMap<Integer, Map<Long, Integer>>();
	// Position -> Value -> Tuple
	private final Map<Integer, Map<Long, Set<Row>>> maps = new HashMap<Integer, Map<Long, Set<Row>>>();

	public Tuples(List<Tuple> tuples, int tuplesLength, List<Integer> steps) {
		this.tuplesLength = tuplesLength;
		rows = new ArrayList<Row>();
		for (int i = 0; i < tuples.size(); ++i) {
			Tuple t = tuples.get(i);
			TLong[] signature = new TLong[tuplesLength];
			for (int j = 0; j < tuplesLength; ++j) {
				signature[j] = new TLong();
			}
			Tuple resultTuple = TupleFactory.newTuple(signature);
			for (int j = 0; j < tuplesLength; j++) {
				SimpleData data = t.get(j);
				resultTuple.set(data, j);
			}
			int step = steps.get(i);
			rows.add(new Row(step, resultTuple));
		}
	}

	public Set<Row> get(int pos, long value) {
		if (maps.containsKey(pos)) {
			Map<Long, Set<Row>> values = maps.get(pos);
			if (values.containsKey(value)) {
				return values.get(value);
			}
		}
		synchronized (this) {
			Map<Long, Set<Row>> map = new HashMap<Long, Set<Row>>();
			for (Row r : rows) {
				Tuple t = r.getTuple();
				long key = ((TLong) t.get(pos)).getValue();
				if (map.containsKey(key)) {
					map.get(key).add(r);
				} else {
					Set<Row> set = new HashSet<Row>();
					set.add(r);
					map.put(key, set);
				}
			}
			maps.put(pos, map);
			return map.get(value);
		}
	}

	public int getNTuples() {
		return rows.size();
	}

	public Row getRow(int pos) {
		return rows.get(pos);
	}

	public SortedSet<Long> getSortedSet(int pos) {
		if (indexes.containsKey(pos)) {
			return indexes.get(pos);
		}
		synchronized (this) {
			TreeSet<Long> set = new TreeSet<Long>();
			for (Row row : rows) {
				Tuple t = row.getTuple();
				long val = ((TLong) t.get(pos)).getValue();
				set.add(val);
			}
			indexes.put(pos, set);
			return set;
		}
	}

	public Map<Long, Integer> getSortedSetWithStep(int pos) {
		if (indexesWithStep.containsKey(pos)) {
			return indexesWithStep.get(pos);
		}
		synchronized (this) {
			Map<Long, Integer> map = new HashMap<Long, Integer>();
			for (Row row : rows) {
				Tuple t = row.getTuple();
				long key = ((TLong) t.get(pos)).getValue();
				int step = row.getStep();
				Integer value = map.get(key);
				if (value != null) {
					step = Math.max(step, value);
				}
				map.put(key, step);
			}
			indexesWithStep.put(pos, map);
			return map;
		}
	}

	public int getTuplesLength() {
		return tuplesLength;
	}

	public Tuples merge(Tuples otherSet) {
		List<Tuple> allTuples = new ArrayList<Tuple>();
		List<Integer> allSteps = new ArrayList<Integer>();
		int length = 0;
		for (Row row : rows) {
			allTuples.add(row.getTuple());
			allSteps.add(row.getStep());
			if (length == 0) {
				length = row.getTuple().getNElements();
			}
		}
		for (Row row : otherSet.rows) {
			allTuples.add(row.getTuple());
			allSteps.add(row.getStep());
			if (length == 0) {
				length = row.getTuple().getNElements();
			}
		}
		return new Tuples(allTuples, length, allSteps);
	}

}
