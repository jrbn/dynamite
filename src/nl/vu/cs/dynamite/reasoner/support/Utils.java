package nl.vu.cs.dynamite.reasoner.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.dynamite.storage.Pattern;
import nl.vu.cs.dynamite.storage.Term;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils extends nl.vu.cs.ajira.utils.Utils {

	static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static Pattern parsePattern(String sp) {
		Pattern p = new Pattern();
		if (sp.endsWith("*")) {
			p.setPrecomputed(true);
			sp = sp.substring(0, sp.length() - 1);
		}
		sp = sp.substring(1, sp.length() - 1);

		String[] sterms = sp.split(" ");
		for (int i = 0; i < 3; ++i) {
			Term t = new Term();
			String st = sterms[i];
			if (st.charAt(0) == '?') {
				t.setName(st.substring(1));
			} else {
				t.setName(null);
				t.setValue(Long.valueOf(st));
			}
			p.setTerm(i, t);
		}

		return p;
	}

	public static final boolean tupleMatchConstants(Tuple tuple,
			int[] pos_constants, long[] value_constants) {
		for (int i = 0; i < pos_constants.length; ++i) {
			TLong t = (TLong) tuple.get(pos_constants[i]);
			if (t.getValue() != value_constants[i])
				return false;
		}
		return true;
	}

	public static int[][] getPositionSharedVariables(Pattern p1, Pattern p2) {
		List<int[]> positions = new ArrayList<int[]>();
		for (int i = 0; i < 3; ++i) {
			Term t1 = p1.getTerm(i);
			if (t1.getName() != null) {
				String s1 = t1.getName();
				for (int j = 0; j < 3; ++j) {
					Term t2 = p2.getTerm(j);
					if (t2.getName() != null) {
						String s2 = t2.getName();
						if (s1.equals(s2)) {
							int[] matching = new int[2];
							matching[0] = i;
							matching[1] = j;
							positions.add(matching);
						}
					}
				}
			}
		}

		return positions.toArray(new int[positions.size()][]);
	}

	public static int[][] getPositionSharedVariables(Pattern p1,
			Collection<String> p2) {
		List<int[]> positions = new ArrayList<int[]>();
		for (int i = 0; i < 3; ++i) {
			Term t1 = p1.getTerm(i);
			if (t1.getName() != null) {
				String s1 = t1.getName();
				int j = 0;
				for (String s2 : p2) {
					if (s1.equals(s2)) {
						int[] matching = new int[2];
						matching[0] = i;
						matching[1] = j;
						positions.add(matching);
					}
					j++;
				}
			}
		}

		return positions.toArray(new int[positions.size()][]);
	}

	public static int[][] getPositionSharedVariables(Collection<String> var1,
			Collection<String> var2) {
		List<int[]> positions = new ArrayList<int[]>();
		int i = 0;
		for (String v1 : var1) {
			int j = 0;
			for (String v2 : var2) {
				if (v1.equals(v2)) {
					int[] matching = new int[2];
					matching[0] = i;
					matching[1] = j;
					positions.add(matching);
				}
				j++;
			}
			i++;
		}
		return positions.toArray(new int[positions.size()][]);
	}

	public static int[][] getPositionNotSharedVariables(
			Collection<String> var1, Collection<String> var2) {
		int[][] pos_shared = getPositionSharedVariables(var1, var2);

		int[][] results = new int[2][];
		// Fill first
		results[0] = new int[var1.size() - pos_shared.length];
		int c1 = 0;
		int c2 = 0;
		for (int i = 0; i < var1.size(); ++i) {
			if (c1 < pos_shared.length && i == pos_shared[c1][0]) {
				++c1;
			} else {
				results[0][c2] = i;
			}
		}

		// Fill second
		results[1] = new int[var2.size() - pos_shared.length];
		c2 = c1 = 0;
		for (int i = 0; i < var2.size(); ++i) {
			if (c1 < pos_shared.length && i == pos_shared[c1][1]) {
				++c1;
			} else {
				results[1][c2] = i;
			}
		}

		return results;
	}

	public static Collection<String> concatenateVariables(List<Pattern> patterns) {
		List<String> variables = new ArrayList<String>();

		// First add the variables of the first pattern
		variables.addAll(patterns.get(0).getVariables());

		// Then add all the variables that do not have appeared so far
		for (int i = 1; i < patterns.size(); ++i) {
			Pattern p = patterns.get(i);
			for (String v : p.getVariables()) {
				if (!variables.contains(v)) {
					variables.add(v);
				}
			}
		}

		return variables;
	}
}