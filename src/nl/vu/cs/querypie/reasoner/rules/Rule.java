package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.querypie.reasoner.support.Pattern;

public class Rule {

	private int id;
	private Pattern head;
	private Pattern[] precomputedPatterns;
	private Pattern[] genericPatterns;

	public Rule(int id, Pattern head, Pattern[] body) {
		this.id = id;
		this.head = head;

		List<Pattern> precomp = new ArrayList<Pattern>();
		List<Pattern> gen = new ArrayList<Pattern>();
		for (Pattern p : body) {
			if (p.isPrecomputed()) {
				precomp.add(p);
			} else {
				gen.add(p);
			}
		}

		precomputedPatterns = precomp.toArray(new Pattern[precomp.size()]);
		genericPatterns = gen.toArray(new Pattern[gen.size()]);
	}

	public int getId() {
		return id;
	}

	public Pattern getHead() {
		return head;
	}

	public Pattern[] getPrecomputedBodyPatterns() {
		return precomputedPatterns;
	}

	public Pattern[] getGenericBodyPatterns() {
		return genericPatterns;
	}
}
