package nl.vu.cs.querypie.reasoner.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils extends nl.vu.cs.ajira.utils.Utils {

	static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static Pattern parsePattern(String sp) {
		Pattern p = new Pattern();
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

		if (sp.endsWith("*")) {
			p.setPrecomputed(true);
		}
		return p;
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

	public static Collection<String> concatenateVariables(Pattern... patterns) {
		List<String> variables = new ArrayList<String>();

		// First add the variables of the first pattern
		variables.addAll(patterns[0].getVariables());

		// Then add all the variables that do not have appeared so far
		for (int i = 1; i < patterns.length; ++i) {
			Pattern p = patterns[i];
			for (String v : p.getVariables()) {
				if (!variables.contains(v)) {
					variables.add(v);
				}
			}
		}

		return variables;
	}
}