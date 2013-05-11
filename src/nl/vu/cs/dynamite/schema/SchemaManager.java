package nl.vu.cs.dynamite.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.dynamite.reasoner.support.Consts;
import nl.vu.cs.dynamite.reasoner.support.Utils;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.TupleSet;
import nl.vu.cs.querypie.storage.inmemory.Tuples;
import nl.vu.cs.querypie.storage.inmemory.Tuples.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaManager {

	static final Logger log = LoggerFactory.getLogger(SchemaManager.class);
	private final InputLayer kb;

	public SchemaManager(InputLayer kb) {
		this.kb = kb;
	}

	public Tuples getTuples(List<Pattern> patterns, ActionContext context,
			boolean flaggedOnly) throws Exception {
		List<Map<String, Integer>> variablesPositions = retrieveVariablesFromPatterns(patterns);
		if (!isCurrentlySupported(variablesPositions)) {
			throw new Exception("Currently not implemented");
		}
		if (patterns.size() == 1) {
			Pattern p = patterns.get(0);
			Tuples tuples = flaggedOnly ? getFlaggedTuples(p, context)
					: getTuples(p, context);
			return tuples;
		} else {
			Pattern p1 = patterns.get(0);
			Pattern p2 = patterns.get(1);
			Tuples allTuples1 = getTuples(p1, context);
			Tuples allTuples2 = getTuples(p2, context);
			if (flaggedOnly) {
				Tuples flaggedTuples1 = getFlaggedTuples(p1, context);
				Tuples flaggedTuples2 = getFlaggedTuples(p2, context);
				Tuples result1 = joinSets(p1.getVariables(), flaggedTuples1,
						p2.getVariables(), allTuples2);
				Tuples result2 = joinSets(p1.getVariables(), allTuples1,
						p2.getVariables(), flaggedTuples2);
				// Merge the two
				return result1.merge(result2);
			} else {
				return joinSets(p1.getVariables(), allTuples1,
						p2.getVariables(), allTuples2);
			}
		}
	}

	private Tuples getTuples(Pattern p, ActionContext context) {
		int nVars = 0;
		int[] pos_vars = new int[3];
		TLong[] query = new TLong[3];

		for (int j = 0; j < 3; ++j) {
			if (p.getTerm(j).getName() != null) {
				query[j] = new TLong(-1);
				pos_vars[nVars++] = j;
			} else {
				query[j] = new TLong(p.getTerm(j).getValue());
			}
		}

		Tuple t = TupleFactory.newTuple(query);
		TupleIterator itr = kb.getIterator(t, context);
		Tuple row = TupleFactory.newTuple(new TLong(), new TLong(),
				new TLong(), new TInt());

		List<Tuple> resultList = new ArrayList<Tuple>();
		List<Integer> steps = new ArrayList<Integer>();
		try {
			while (itr != null && itr.isReady() && itr.nextTuple()) {
				itr.getTuple(row);
				TLong[] signature = new TLong[nVars];
				for (int i = 0; i < nVars; ++i)
					signature[i] = new TLong();
				Tuple resultTuple = TupleFactory.newTuple(signature);
				for (int i = 0; i < nVars; ++i) {
					resultTuple.set(row.get(pos_vars[i]), i);
				}
				resultList.add(resultTuple);
				steps.add(((TInt) row.get(3)).getValue());
			}
		} catch (Exception e) {
			log.error("Error", e);
		}

		Tuples tuples = new Tuples(resultList, nVars, steps);
		return tuples;
	}

	private Tuples getFlaggedTuples(Pattern p, ActionContext context) {
		TupleSet inMemorySet = (TupleSet) context
				.getObjectFromCache(Consts.CURRENT_DELTA_KEY);
		if (inMemorySet == null) {
			return null;
		}
		Set<Tuple> result = null;
		try {
			result = inMemorySet.getSubset(p);
		} catch (Exception e) {
			log.error("Error", e);
		}
		// Determine position of variables
		int[] pos_vars = p.getPositionVariables();
		List<Tuple> resultList = new ArrayList<Tuple>();
		List<Integer> steps = new ArrayList<Integer>();
		for (Tuple t : result) {
			SimpleData[] data = new SimpleData[pos_vars.length];
			for (int i = 0; i < pos_vars.length; ++i) {
				data[i] = t.get(pos_vars[i]);
			}
			Tuple resultTuple = TupleFactory.newTuple(data);
			resultList.add(resultTuple);
			steps.add(Integer.MAX_VALUE);
		}
		return new Tuples(resultList, pos_vars.length, steps);
	}

	private boolean isCurrentlySupported(
			List<Map<String, Integer>> variablesPositions) {
		if (variablesPositions.size() < 1 || variablesPositions.size() > 2) {
			return false;
		}
		if (variablesPositions.size() == 1) {
			return true;
		} else if (variablesPositions.size() == 2) {
			Map<String, Integer> pattern1 = variablesPositions.get(0);
			Map<String, Integer> pattern2 = variablesPositions.get(1);
			if (pattern1.size() != 2 || pattern2.size() != 2)
				return false;
			int numConstraints = 0;
			for (String key1 : pattern1.keySet()) {
				for (String key2 : pattern2.keySet()) {
					if (key1.equals(key2)) {
						numConstraints++;
					}
				}
			}
			return (numConstraints == 1);
		} else {
			return false;
		}
	}

	private Tuples joinSets(Collection<String> var1, Tuples t1,
			Collection<String> var2, Tuples t2) throws Exception {
		// Determine position of variables
		int[][] pos_shared_vars = Utils.getPositionSharedVariables(var1, var2);
		int[][] pos_not_shared_vars = Utils.getPositionNotSharedVariables(var1,
				var2);
		int sizeTupleResult = var1.size() + var2.size()
				- pos_shared_vars.length;
		if (pos_shared_vars.length > 1) {
			throw new Exception("Not supported");
		}
		List<Tuple> resultList = new ArrayList<Tuple>();
		List<Integer> steps = new ArrayList<Integer>();
		int l1 = t1.getTuplesLength();
		for (int i = 0; i < t1.getNTuples(); ++i) {
			Row r1 = t1.getRow(i);
			Set<Row> r2s = t2.get(pos_shared_vars[0][1],
					r1.getValue(pos_shared_vars[0][0]).getValue());
			if (r2s != null) {
				for (Row r2 : r2s) {

					TLong[] signature = new TLong[sizeTupleResult];
					for (int m = 0; m < sizeTupleResult; ++m) {
						signature[m] = new TLong();
					}

					Tuple resultTuple = TupleFactory.newTuple(signature);
					int currentPosition = 0;
					for (int j = 0; j < l1; ++j) {
						resultTuple.set(r1.getValue(j), currentPosition++);
					}
					for (int j = 0; j < pos_not_shared_vars[1].length; ++j) {
						resultTuple.set(r2.getValue(pos_not_shared_vars[1][j]),
								currentPosition++);
					}
					resultList.add(resultTuple);
					steps.add(Math.max(r1.getStep(), r2.getStep()));
				}
			}
		}
		return new Tuples(resultList, sizeTupleResult, steps);
	}

	private Map<String, Integer> retrieveVariablesFromPatter(Pattern p) {
		Map<String, Integer> variables = new HashMap<String, Integer>();
		for (int i = 0; i < 3; i++) {
			Term term = p.getTerm(i);
			String name = term.getName();
			if (name != null) {
				variables.put(name, i);
			}
		}
		return variables;
	}

	private List<Map<String, Integer>> retrieveVariablesFromPatterns(
			List<Pattern> patterns) {
		List<Map<String, Integer>> variablesPositions = new ArrayList<Map<String, Integer>>();
		for (Pattern p : patterns) {
			Map<String, Integer> map = retrieveVariablesFromPatter(p);
			variablesPositions.add(map);
		}
		return variablesPositions;
	}
}
