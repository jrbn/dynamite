package nl.vu.cs.dynamite.reasoner.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.dynamite.ReasoningContext;
import nl.vu.cs.dynamite.reasoner.support.Utils;
import nl.vu.cs.dynamite.storage.Pattern;
import nl.vu.cs.dynamite.storage.Term;
import nl.vu.cs.dynamite.storage.inmemory.Tuples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule {

	static final Logger log = LoggerFactory.getLogger(Rule.class);

	private final int id;
	private final Pattern head;
	private final List<Pattern> precomputedPatterns;
	private final List<Pattern> genericPatterns;

	// Contains the set of precomputed triples
	private boolean invalidAllPrecomputedSet = true;
	private Tuples allPrecomputedTuples = null;
	private boolean invalidFlaggedPrecomputedSet = true;
	private Tuples flaggedPrecomputedTuples = null;

	// Positions shared variables in the first generic pattern that are used in
	// the head
	private final int[][] pos_gen_head;

	// Positions shared variables of the precomputed patters that appear in the
	// head of the rule
	private final int[][] pos_head_precomp;

	// Positions of the shared variables between the first generic pattern and
	// the precomputed triples. This is used to filter generic triples that will
	// not produce any derivation
	private final int[][] pos_gen_precomp;

	// Positions of the constants in the generic pattern (used for matching
	// later on)
	private final int[] constant_positions;
	private final long[] constant_values;

	public Rule(int id, Pattern head, Pattern[] body) {
		this.id = id;
		this.head = head;
		precomputedPatterns = new ArrayList<Pattern>();
		genericPatterns = new ArrayList<Pattern>();
		for (Pattern p : body) {
			if (p.isPrecomputed()) {
				precomputedPatterns.add(p);
			} else {
				genericPatterns.add(p);
			}
		}

		Collection<String> precomputedSignatures = null;
		if (!precomputedPatterns.isEmpty()) {
			// If there are more precomputed patterns, precompute the join in
			// memory
			precomputedSignatures = Utils
					.concatenateVariables(precomputedPatterns);
			// Calculate the positions of the precomputed patterns that appear
			// in the head
			pos_head_precomp = Utils.getPositionSharedVariables(head,
					precomputedSignatures);
		} else {
			pos_head_precomp = null;
		}

		if (!genericPatterns.isEmpty()) {
			// Calculate the positions of the shared variables between the head
			// and the first generic pattern (it will be the key of the "map"
			// phase)
			pos_gen_head = Utils.getPositionSharedVariables(
					genericPatterns.get(0), head);
			// Calculate the positions of the shared variables between the first
			// generic pattern and the precomputed triples
			if (!precomputedPatterns.isEmpty()) {
				pos_gen_precomp = Utils.getPositionSharedVariables(
						genericPatterns.get(0), precomputedSignatures);
			} else {
				pos_gen_precomp = null;
			}

			int[] pc = new int[3];
			int count = 0;
			long[] vc = new long[3];
			for (int i = 0; i < 3; ++i) {
				Term t = genericPatterns.get(0).getTerm(i);
				if (t.getName() == null) {
					pc[count] = i;
					vc[count++] = t.getValue();
				}
			}
			constant_positions = Arrays.copyOf(pc, count);
			constant_values = Arrays.copyOf(vc, count);
		} else {
			pos_gen_head = null;
			pos_gen_precomp = null;
			constant_positions = null;
			constant_values = null;
		}
	}

	public int getId() {
		return id;
	}

	public Pattern getHead() {
		return head;
	}

	public List<Pattern> getPrecomputedBodyPatterns() {
		return precomputedPatterns;
	}

	public List<Pattern> getGenericBodyPatterns() {
		return genericPatterns;
	}

	public int[][] getSharedVariablesGen_Precomp() {
		return pos_gen_precomp;
	}

	public int[][] getSharedVariablesGen_Head() {
		return pos_gen_head;
	}

	public int[][] getSharedVariablesHead_Precomp() {
		return pos_head_precomp;
	}

	public Tuples getAllPrecomputedTuples(ActionContext context) {
		if (invalidAllPrecomputedSet && precomputedPatterns != null
				&& precomputedPatterns.size() > 0) {
			try {
				ReasoningContext c = ReasoningContext.getInstance();
				allPrecomputedTuples = c.getSchemaManager().getTuples(
						precomputedPatterns, context, false);
			} catch (Exception e) {
				log.error("Failed reloading", e);
			}
			invalidAllPrecomputedSet = false;
		}

		return allPrecomputedTuples;
	}

	public Tuples getFlaggedPrecomputedTuples(ActionContext context) {
		if (invalidFlaggedPrecomputedSet && precomputedPatterns != null
				&& precomputedPatterns.size() > 0) {
			try {
				ReasoningContext c = ReasoningContext.getInstance();
				flaggedPrecomputedTuples = c.getSchemaManager().getTuples(
						precomputedPatterns, context, true);
			} catch (Exception e) {
				log.error("Failed reloading", e);
			}
			invalidFlaggedPrecomputedSet = false;
		}

		return flaggedPrecomputedTuples;
	}

	public void invalidatePrecomputation() {
		invalidAllPrecomputedSet = true;
		invalidFlaggedPrecomputedSet = true;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Rule)) {
			return false;
		}
		return id == ((Rule) obj).getId();
	}

	public boolean isSchemaOnly() {
		return genericPatterns.isEmpty();
	}

	public boolean isGenericOnly() {
		return precomputedPatterns.isEmpty();
	}

	public int[] getPositionsConstantGenericPattern() {
		return constant_positions;
	}

	public long[] getValueConstantGenericPattern() {
		return constant_values;
	}
}
