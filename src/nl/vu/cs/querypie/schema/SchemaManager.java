package nl.vu.cs.querypie.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.sets.Tuples;
import nl.vu.cs.querypie.storage.berkeleydb.BerkeleydbLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

public class SchemaManager {

	static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

	private final BerkeleydbLayer kb;

	public SchemaManager(BerkeleydbLayer kb) {
		this.kb = kb;
	}

	public Tuples getTuples(Pattern[] patterns, ActionContext context) throws Exception {
		// Retrieve the triples for each pattern
		long[][] tuples = new long[patterns.length][];
		int[][] pos_vars = new int[patterns.length][];

		TLong[] query = new TLong[3];
		for (int i = 0; i < patterns.length; ++i) {
			Pattern p = patterns[i];
			// Get the triples
			int nvars = 0;
			int[] posToCopy = new int[3];
			for (int j = 0; j < 3; ++j) {
				if (p.getTerm(j).getName() != null) {
					query[j] = new TLong(-1);
					posToCopy[nvars++] = j;
				} else {
					query[j] = new TLong(p.getTerm(j).getValue());
				}
			}
			pos_vars[i] = Arrays.copyOf(posToCopy, nvars);
			Tuple t = TupleFactory.newTuple(query);
			TupleIterator itr = kb.getIterator(t, context);

			// Copy the bindings on a new data structure
			ArrayList<Long> rawValues = new ArrayList<Long>();
			try {
				while (itr != null && itr.isReady() && itr.nextTuple()) {
					itr.getTuple(t);
					for (int m = 0; m < nvars; ++m) {
						Long val = ((TLong) t.get(posToCopy[m])).getValue();
						rawValues.add(val);
					}
				}
			} catch (Exception e) {
				log.error("Error", e);
			}
			tuples[i] = Longs.toArray(rawValues);
		}

		if (patterns.length == 1) {
			Tuples output = new Tuples(pos_vars[0].length, tuples[0]);
			return output;
		}

		if (!isCurrentlySupported(pos_vars, patterns)) throw new Exception("Not Implemented");

		// If more than one pattern, than join the bindings using a hash join
		long[] currentResults = tuples[0];
		Map<Integer, String> currentNames = new HashMap<Integer, String>();
		for (int i = 0; i < pos_vars[0].length; i++) {
			Integer key = pos_vars[0][i];
			String val = patterns[0].getTerm(key).getName();
			currentNames.put(key, val);
		}
		for (int i = 1; i < patterns.length; ++i) {
			Pattern p = patterns[i];

			// Retrieve the position of the variables to use for join
			int var1 = -1;	// Position of the variable to join in pattern1
			int var2 = -1;	// Position of the variable to join in pattern2
			outerloop: for (Integer index : currentNames.keySet()) {
				String name1 = currentNames.get(index);
				for (int j = 0; j < pos_vars[i].length; ++j) {
					String name2 = p.getTerm(j).getName();
					if (name1.equals(name2)) {
						var1 = index;
						var2 = j;
						break outerloop;
					}
				}
			}
			// Currently we assume that one and only one join condition is specified
			assert (var1 >= 0 && var2 >= 0);

			// Order tuples according to the joining variable
			Map<Long, Set<Long>> firstMap = new HashMap<Long, Set<Long>>();
			Map<Long, Set<Long>> secondMap = new HashMap<Long, Set<Long>>();
			for (int j = 0; j < currentResults.length; j += 2) {
				Long key = currentResults[j + var1];
				Long val = currentResults[j + 1 - var1];
				if (firstMap.containsKey(key)) {
					firstMap.get(key).add(val);
				} else {
					Set<Long> newSet = new HashSet<Long>();
					newSet.add(val);
					firstMap.put(key, newSet);
				}
			}
			for (int j = 0; j < tuples[i].length; j += 2) {
				Long key = tuples[i][j + var2];
				Long val = tuples[i][j + 1 - var2];
				if (secondMap.containsKey(key)) {
					secondMap.get(key).add(val);
				} else {
					Set<Long> newSet = new HashSet<Long>();
					newSet.add(val);
					secondMap.put(key, newSet);
				}
			}

			// Execute the join
			List<Long> resultList = new ArrayList<Long>();
			for (Long key : firstMap.keySet()) {
				if (!secondMap.containsKey(key)) continue;
				for (Long freeVal1 : firstMap.get(key)) {
					for (Long freeVal2 : secondMap.get(key)) {
						resultList.add(freeVal1);
						resultList.add(freeVal2);
					}
				}
			}

			// Update current results
			currentResults = Longs.toArray(resultList);

			// Update current names
			currentNames.clear();
			currentNames.put(0, patterns[i - 1].getTerm(1 - var1).getName());
			currentNames.put(1, patterns[i - 1].getTerm(1 - var2).getName());

		}

		return new Tuples(2, currentResults);
	}

	private boolean isCurrentlySupported(int[][] pos_vars, Pattern[] patterns) {
		for (int i = 0; i < pos_vars.length; i++) {
			if (pos_vars[i].length != 2) return false;
		}
		for (int i = 0; i < patterns.length - 1; i++) {
			String p1Name1 = patterns[i].getTerm(pos_vars[i][0]).getName();
			String p1Name2 = patterns[i].getTerm(pos_vars[i][1]).getName();
			if (p1Name1.equals(p1Name2)) return false;
			String p2Name1 = patterns[i].getTerm(pos_vars[i + 1][0]).getName();
			String p2Name2 = patterns[i].getTerm(pos_vars[i + 1][1]).getName();
			if (p2Name1.equals(p2Name2)) return false;
			int numConstraints = 0;
			if (p1Name1.equals(p2Name1)) numConstraints++;
			if (p1Name1.equals(p2Name2)) numConstraints++;
			if (p1Name2.equals(p2Name2)) numConstraints++;
			if (numConstraints > 0) return false;
		}
		return true;
	}
}
