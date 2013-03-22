package nl.vu.cs.querypie.reasoner.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.support.Utils;
import nl.vu.cs.querypie.storage.Pattern;
import nl.vu.cs.querypie.storage.Term;
import nl.vu.cs.querypie.storage.inmemory.Tuples;

public class Rule {

	private final int id;
	private final Pattern head;
	private final List<Pattern> precomputedPatterns;
	private final List<Pattern> genericPatterns;

	// Contains the set of precomputed triples
	private Tuples allPrecomputedTuples = null;
	private Tuples flaggedPrecomputedTuples = null;
	private Collection<String> precomputedSignatures = null;

	// Positions shared variables in the first generic pattern that are used in
	// the head
	private int[][] pos_gen_head = null;

	// Positions shared variables of the precomputed patters that appear in the
	// head of the rule
	private int[][] pos_head_precomp = null;

	// Positions of the shared variables between the first generic pattern and
	// the precomputed triples. This is used to filter generic triples that will
	// not produce any derivation
	private int[][] pos_gen_precomp = null;

	// Positions of the constants in the generic pattern (used for matching
	// later on)
	private int[] constant_positions = null;
	private long[] constant_values = null;

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

	public Tuples getAllPrecomputedTuples() {
		return allPrecomputedTuples;
	}

	public Tuples getFlaggedPrecomputedTuples() {
		return flaggedPrecomputedTuples;
	}

	public void reloadPrecomputation(ReasoningContext c, ActionContext context,
			boolean flaggedOnly) {

		if (precomputedPatterns.isEmpty())
			return;
		try {
			if (flaggedOnly) {
				flaggedPrecomputedTuples = c.getSchemaManager().getTuples(
						precomputedPatterns, context, flaggedOnly);
			} else {
				allPrecomputedTuples = c.getSchemaManager().getTuples(
						precomputedPatterns, context);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

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

	public void init(ReasoningContext c) {
		if (!precomputedPatterns.isEmpty()) {
			// If there are more precomputed patterns, precompute the join in
			// memory
			precomputedSignatures = Utils
					.concatenateVariables(precomputedPatterns);
			// Calculate the positions of the precomputed patterns that appear
			// in the head
			pos_head_precomp = Utils.getPositionSharedVariables(head,
					precomputedSignatures);
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
		}
	}

	public int[] getPositionsConstantGenericPattern() {
		return constant_positions;
	}

	public long[] getValueConstantGenericPattern() {
		return constant_values;
	}
}
