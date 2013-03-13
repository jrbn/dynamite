package nl.vu.cs.querypie.reasoner.actions;

import java.util.Collection;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.querypie.ReasoningContext;
import nl.vu.cs.querypie.reasoner.rules.Rule;
import nl.vu.cs.querypie.reasoner.support.Pattern;
import nl.vu.cs.querypie.reasoner.support.Term;
import nl.vu.cs.querypie.reasoner.support.sets.Tuples;

public class RuleExecutor2 extends Action {

	public static final int RULE_ID = 0;

	private int[][] pos_head_precomps;
	private int[][] pos_gen_precomps;
	private int[][] pos_gen_head;
	private Tuples precompTuples;
	private final SimpleData[] outputTriple = new SimpleData[3];
	private final TLong[] supportTriple = { new TLong(), new TLong(),
			new TLong() };

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(RULE_ID, "rule", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// Get the rule
		Rule[] rules = ReasoningContext.getInstance().getRuleset()
				.getAllRulesWithSchemaAndGeneric();
		if (rules == null) {
			return;
		}
		Rule rule = rules[0];

		pos_head_precomps = rule.getSharedVariablesHead_Precomp();
		pos_gen_precomps = rule.getSharedVariablesGen_Precomp();
		pos_gen_head = rule.getSharedVariablesGen_Head();
		precompTuples = rule.getPrecomputedTuples();

		if (pos_gen_precomps.length > 1) {
			throw new Exception("Not supported");
		}

		// Set up the the triple that should be returned in output
		for (int i = 0; i < pos_head_precomps.length; ++i) {
			outputTriple[pos_head_precomps[i][0]] = supportTriple[i];
		}

		// Fill the outputTriple with the constants that come from the head of
		// the rule
		Pattern head = rule.getHead();
		for (int i = 0; i < 3; ++i) {
			Term t = head.getTerm(i);
			if (t.getName() == null) {
				outputTriple[i] = new TLong(t.getValue());
			}
		}
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {

		// First copy the "key" in the output triple.
		for (int i = 0; i < pos_gen_head.length; ++i) {
			outputTriple[pos_gen_head[i][1]] = tuple.get(i);
		}

		// Perform the join between the "value" part of the triple and the
		// precomputed tuples. Notice that this works only if there is one
		// element to join.
		TBag values = (TBag) tuple.get(tuple.getNElements() - 1);
		for (Tuple t : values) {
			TLong elementToJoin = (TLong) t.get(0);
			Collection<long[]> set = precompTuples.get(pos_gen_precomps[0][1],
					elementToJoin.getValue());
			if (set != null) {
				for (long[] row : set) {
					// Get current values
					for (int i = 0; i < pos_head_precomps.length; ++i) {
						supportTriple[i].setValue(row[pos_head_precomps[i][1]]);
					}
					actionOutput.output(outputTriple);
				}
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		pos_head_precomps = null;
		pos_gen_precomps = null;
		pos_gen_head = null;
		precompTuples = null;
	}
}
